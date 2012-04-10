require 'mongo'

require 'resque/version'

require 'resque/errors'

require 'resque/failure'
require 'resque/failure/base'

require 'resque/helpers'
require 'resque/stat'
require 'resque/job'
require 'resque/worker'
require 'resque/plugin'

module Resque
  include Helpers
  extend self
  @delayed_queues = []
  
  attr_accessor :delayed_queues
  
  # Set the queue database. Expects a Mongo::DB object.
  def mongo=(database)
    if database.is_a? Mongo::DB
      @mongo = database
    elsif database.is_a? String
      @mongo = Mongo::Connection.new.db(database)
    else
      raise ArgumentError, "Resque.mongo= expects a Mongo::DB database or a string representing the name of the database, not a #{database.class}."
    end
    initialize_mongo
  end

  # Returns the current Mongo::DB. If none has been created, it will
  # create a new one called 'resque'.
  def mongo
    return @mongo if @mongo
    self.mongo = Mongo::Connection.new.db("resque")
    @mongo
  end

  def initialize_mongo
    mongo_workers.create_index :worker
    mongo_stats.create_index :stat
    delayed_queues = mongo_stats.find_one(:stat => 'Delayed Queues')
    @delayed_queues = delayed_queues['value'] if delayed_queues
  end

  def mongo_workers
    mongo['resque.workers']
  end

  def mongo_stats
    mongo['resque.metrics']
  end

  def mongo_failures
    mongo['resque.failures']
  end

  # The `before_first_fork` hook will be run in the **parent** process
  # only once, before forking to run the first job. Be careful- any
  # changes you make will be permanent for the lifespan of the
  # worker.
  #
  # Call with a block to set the hook.
  # Call with no arguments to return the hook.
  def before_first_fork(&block)
    block ? (@before_first_fork = block) : @before_first_fork
  end

  # Set a proc that will be called in the parent process before the
  # worker forks for the first time.
  def before_first_fork=(before_first_fork)
    @before_first_fork = before_first_fork
  end

  # The `before_fork` hook will be run in the **parent** process
  # before every job, so be careful- any changes you make will be
  # permanent for the lifespan of the worker.
  #
  # Call with a block to set the hook.
  # Call with no arguments to return the hook.
  def before_fork(&block)
    block ? (@before_fork = block) : @before_fork
  end

  # Set the before_fork proc.
  def before_fork=(before_fork)
    @before_fork = before_fork
  end

  # The `after_fork` hook will be run in the child process and is passed
  # the current job. Any changes you make, therefore, will only live as
  # long as the job currently being processed.
  #
  # Call with a block to set the hook.
  # Call with no arguments to return the hook.
  def after_fork(&block)
    block ? (@after_fork = block) : @after_fork
  end

  # Set the after_fork proc.
  def after_fork=(after_fork)
    @after_fork = after_fork
  end

  def to_s
    connection_info = mongo.connection.primary_pool
    "Resque Client connected to #{connection_info.host}:#{connection_info.port}/#{mongo.name}"
  end

  def delayed_job?(klass)
    klass.instance_variable_get(:@delayed) ||
      (klass.respond_to?(:delayed) and klass.delayed)
  end

  def delayed_queue?(queue)
    @delayed_queues.include? namespace_queue(queue)
  end

  def enable_delay(queue)
    queue = namespace_queue(queue)
    unless delayed_queue? queue
      @delayed_queues << queue
      mongo_stats.update({:stat => 'Delayed Queues'}, {'$addToSet' => {'value' => queue}}, {:upsert => true})
    end
  end
  
  # If 'inline' is true Resque will call #perform method inline
  # without queuing it into Redis and without any Resque callbacks.
  # The 'inline' is false Resque jobs will be put in queue regularly.
  def inline?
    @inline
  end
  alias_method :inline, :inline?

  def inline=(inline)
    @inline = inline
  end

  #
  # queue manipulation
  #

  # Pushes a job onto a queue. Queue name should be a string and the
  # item should be any JSON-able Ruby object.
  #
  # Resque works generally expect the `item` to be a hash with the following
  # keys:
  #
  #   class - The String name of the job to run.
  #    args - An Array of arguments to pass the job. Usually passed
  #           via `class.to_class.perform(*args)`.
  #
  # Example
  #
  #   Resque.push('archive', :class => 'Archive', :args => [ 35, 'tar' ])
  #
  # Returns nothing
  def push(queue, item)
    queue = namespace_queue(queue)
    item[:resque_enqueue_timestamp] = Time.now
    puts(item.inspect)
    mongo[queue] << item
  end

  # Pops a job off a queue. Queue name should be a string.
  #
  # Returns a Ruby object.
  def pop(queue)
    queue = namespace_queue(queue)
    query = {}
    query['delay_until'] = { '$lt' => Time.now } if delayed_queue?(queue)
    #sorting will result in significant performance penalties for large queues, you have been warned.
    item = mongo[queue].find_and_modify(:query => query, :remove => true, :sort => [[:_id, :asc]])
  rescue Mongo::OperationFailure => e
    return nil if e.message =~ /No matching object/
    raise e
  end

  # Returns an integer representing the size of a queue.
  # Queue name should be a string.
  def size(queue)
    queue = namespace_queue(queue)
    mongo[queue].count
  end

  def delayed_size(queue)
    queue = namespace_queue(queue)
    if delayed_queue? queue
      mongo[queue].find({'delay_until' => { '$gt' => Time.now }}).count
    else
      mongo[queue].count
    end
  end

  def ready_size(queue)
    queue = namespace_queue(queue)
    if delayed_queue? queue
      mongo[queue].find({'delay_until' => { '$lt' => Time.now }}).count
    else
      mongo[queue].count
    end
  end


  # Returns an array of items currently queued. Queue name should be
  # a string.
  #
  # start and count should be integer and can be used for pagination.
  # start is the item to begin, count is how many items to return.
  #
  # To get the 3rd page of a 30 item, paginatied list one would use:
  #   Resque.peek('my_list', 59, 30)
  def peek(queue, start = 0, count = 1, mode = :ready)
    list_range(queue, start, count, mode)
  end

  # Does the dirty work of fetching a range of items from a Redis list
  # and converting them into Ruby objects.
  def list_range(key, start = 0, count = 1, mode = :ready)
    query = { }
    sort = []
    if delayed_queue? key
      if mode == :ready
        query['delay_until'] = { '$not' => { '$gt' => Time.new}}
      elsif mode == :delayed
        query['delay_until'] = { '$gt' => Time.new}
      elsif mode == :delayed_sorted
        query['delay_until'] = { '$gt' => Time.new}
        sort << ['delay_until', 1]
      elsif mode == :all_sorted
        query = {}
        sort << ['delay_until', 1]
      end
    end
    queue = namespace_queue(key)
    items = mongo[queue].find(query, { :limit => count, :skip => start, :sort => sort}).to_a.map{ |i| i}
    count > 1 ? items : items.first
  end

  # Returns an array of all known Resque queues as strings.
  def queues        
    mongo.collection_names.
      select { |name| name =~ /resque\.queues\./ }.
      collect { |name| name.split(".")[2..-1].join('.') }
  end

  # Returns the mongo collection for a given queue
  def collection_for_queue(queue)
    queue = namespace_queue(queue)
    mongo[queue]
  end

  # Given a queue name, completely deletes the queue.
  def remove_queue(queue)
    queue = namespace_queue(queue)
    mongo[queue].drop
  end
  
  def clear_delayed_queues!
    
  end

  #
  # job shortcuts
  #

  # This method can be used to conveniently add a job to a queue.
  # It assumes the class you're passing it is a real Ruby class (not
  # a string or reference) which either:
  #
  #   a) has a @queue ivar set
  #   b) responds to `queue`
  #
  # If either of those conditions are met, it will use the value obtained
  # from performing one of the above operations to determine the queue.
  #
  # If no queue can be inferred this method will raise a `Resque::NoQueueError`
  #
  # Returns true if the job was queued, nil if the job was rejected by a
  # before_enqueue hook.
  #
  # This method is considered part of the `stable` API.
  def enqueue(klass, *args)
    enqueue_to(queue_from_class(klass), klass, *args)
  end

  # Just like `enqueue` but allows you to specify the queue you want to
  # use. Runs hooks.
  #
  # `queue` should be the String name of the queue you're targeting.
  #
  # Returns true if the job was queued, nil if the job was rejected by a
  # before_enqueue hook.
  #
  # This method is considered part of the `stable` API.
  def enqueue_to(queue, klass, *args)
    # Perform before_enqueue hooks. Don't perform enqueue if any hook returns false
    before_hooks = Plugin.before_enqueue_hooks(klass).collect do |hook|
      klass.send(hook, *args)
    end
    return nil if before_hooks.any? { |result| result == false }

    Job.create(queue, klass, *args)

    Plugin.after_enqueue_hooks(klass).each do |hook|
      klass.send(hook, *args)
    end

    return true
  end
  
  def enqueue_delayed(klass, *args)
    
  end

  # This method can be used to conveniently remove a job from a queue.
  # It assumes the class you're passing it is a real Ruby class (not
  # a string or reference) which either:
  #
  #   a) has a @queue ivar set
  #   b) responds to `queue`
  #
  # If either of those conditions are met, it will use the value obtained
  # from performing one of the above operations to determine the queue.
  #
  # If no queue can be inferred this method will raise a `Resque::NoQueueError`
  #
  # If no args are given, this method will dequeue *all* jobs matching
  # the provided class. See `Resque::Job.destroy` for more
  # information.
  #
  # Returns the number of jobs destroyed.
  #
  # Example:
  #
  #   # Removes all jobs of class `UpdateNetworkGraph`
  #   Resque.dequeue(GitHub::Jobs::UpdateNetworkGraph)
  #
  #   # Removes all jobs of class `UpdateNetworkGraph` with matching args.
  #   Resque.dequeue(GitHub::Jobs::UpdateNetworkGraph, 'repo:135325')
  #
  # This method is considered part of the `stable` API.
  def dequeue(klass, *args)
    # Perform before_dequeue hooks. Don't perform dequeue if any hook returns false
    before_hooks = Plugin.before_dequeue_hooks(klass).collect do |hook|
      klass.send(hook, *args)
    end
    return if before_hooks.any? { |result| result == false }

    Job.destroy(queue_from_class(klass), klass, *args)

    Plugin.after_dequeue_hooks(klass).each do |hook|
      klass.send(hook, *args)
    end
  end

  # Given a class, try to extrapolate an appropriate queue based on a
  # class instance variable or `queue` method.
  def queue_from_class(klass)
    klass.instance_variable_get(:@queue) ||
      (klass.respond_to?(:queue) and klass.queue)
  end

  # This method will return a `Resque::Job` object or a non-true value
  # depending on whether a job can be obtained. You should pass it the
  # precise name of a queue: case matters.
  #
  # This method is considered part of the `stable` API.
  def reserve(queue)
    Job.reserve(queue)
  end

  # Validates if the given klass could be a valid Resque job
  #
  # If no queue can be inferred this method will raise a `Resque::NoQueueError`
  #
  # If given klass is nil this method will raise a `Resque::NoClassError`
  def validate(klass, queue = nil)
    queue ||= queue_from_class(klass)

    if !queue
      raise NoQueueError.new("Jobs must be placed onto a queue.")
    end

    if klass.to_s.empty?
      raise NoClassError.new("Jobs must be given a class.")
    end
  end


  #
  # worker shortcuts
  #

  # A shortcut to Worker.all
  def workers
    Worker.all
  end

  # A shortcut to Worker.working
  def working
    Worker.working
  end

  # A shortcut to unregister_worker
  # useful for command line tool
  def remove_worker(worker_id)
    worker = Resque::Worker.find(worker_id)
    worker.unregister_worker
  end

  #
  # stats
  #

  # Returns a hash, similar to redis-rb's #info, of interesting stats.
  def info
    return {
      :pending   => queues.inject(0) { |m,k| m + size(k) },
      :processed => Stat[:processed],
      :queues    => queues.size,
      :workers   => workers.size.to_i,
      :working   => working.count,
      :failed    => Stat[:failed],
      :servers   => to_s,
      :environment  => ENV['RAILS_ENV'] || ENV['RACK_ENV'] || 'development'
    }
  end

  # Returns an array of all known Resque keys in Redis. Redis' KEYS operation
  # is O(N) for the keyspace, so be careful - this can be slow for big databases.
  def keys
    names = mongo.collection_names
  end

  def drop
    mongo.collections.each{ |collection| collection.drop unless collection.name =~ /^system./ }
    @mongo = nil
  end

  private
  def namespace_queue(queue)
    queue = queue.to_s
    if queue.start_with?('resque.queues.')
      queue
    else
      "resque.queues.#{queue}"
    end
  end
end

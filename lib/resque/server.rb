require 'sinatra/base'
require 'erb'
require 'resque'
require 'resque/version'
require 'time'

module Resque
  class Server < Sinatra::Base
    dir = File.dirname(File.expand_path(__FILE__))

    set :views,  "#{dir}/server/views"
    set :public_folder, "#{dir}/server/public"
    set :static, true

    helpers do
      include Rack::Utils
      alias_method :h, :escape_html

      def current_section
        url_path request.path_info.sub('/','').split('/')[0].downcase
      end

      def current_page
        url_path request.path_info.sub('/','')
      end

      def url_path(*path_parts)
        [ path_prefix, path_parts ].join("/").squeeze('/')
      end
      alias_method :u, :url_path

      def path_prefix
        request.env['SCRIPT_NAME']
      end

      def class_if_current(path = '')
        'class="current"' if current_page[0, path.size] == path
      end

      def tab(name)
        dname = name.to_s.downcase
        path = url_path(dname)
        "<li #{class_if_current(path)}><a href='#{path}'>#{name}</a></li>"
      end

      def tabs
        Resque::Server.tabs
      end

      def mongo_get_size(key)
        Resque.mongo[key].count
      end

      def mongo_get_value_as_array(key, start=0)
        Resque.mongo[key].find({ }, { :skip => start, :limit => 20}).to_a
      end

      def show_args(args)
        Array(args).map { |a| a.inspect }.join("\n")
      end

      def worker_hosts
        @worker_hosts ||= worker_hosts!
      end

      def worker_hosts!
        hosts = Hash.new { [] }

        Resque.workers.each do |worker|
          host, _ = worker.to_s.split(':')
          hosts[host] += [worker.to_s]
        end

        hosts
      end

      def partial?
        @partial
      end

      def partial(template, local_vars = {})
        @partial = true
        erb(template.to_sym, {:layout => false}, local_vars)
      ensure
        @partial = false
      end

      def poll
        if @polling
          text = "Last Updated: #{Time.now.strftime("%H:%M:%S")}"
        else
          text = "<a href='#{u(request.path_info)}.poll' rel='poll'>Live Poll</a>"
        end
        "<p class='poll'>#{text}</p>"
      end

    end

    def show(page, layout = true)
      response["Cache-Control"] = "max-age=0, private, must-revalidate"
      begin
        erb page.to_sym, {:layout => layout}, :resque => Resque
      rescue Mongo::ConnectionError, Mongo::ConnectionFailure
        erb :error, {:layout => false}, :error => "Can't connect to MongoDB!"
      end
    end

    def show_for_polling(page)
      content_type "text/html"
      @polling = true
      show(page.to_sym, false).gsub(/\s{1,}/, ' ')
    end

    def processes_in(delay_until)
      return 'Immediately' if delay_until.nil?
      now = Time.now
      time = distance_of_time_in_words(now, delay_until)
      return "Immediately (#{time})" if now > delay_until
      return time
    end

    def enqueued_at(resque_enqueue_timestamp)
      return 'Unknown' if resque_enqueue_timestamp.nil?
      now = Time.now
      time = distance_of_time_in_words(now, resque_enqueue_timestamp)
      return time
    end
    
    def distance_of_time_in_words(from_time, to_time = 0, include_seconds = true, options = {})
      from_time = from_time.to_time if from_time.respond_to?(:to_time)
      to_time = to_time.to_time if to_time.respond_to?(:to_time)
      distance_in_minutes = (((to_time - from_time).abs)/60).round
      distance_in_seconds = ((to_time - from_time).abs).round

      ago = from_time > to_time ? ' ago' : ''
      
      
      case distance_in_minutes
      when 0..1
        return distance_in_minutes == 0 ?
        "less than 1 minute" + ago :
          "#{distance_in_minutes} minutes" + ago unless include_seconds
        
        case distance_in_seconds
        when 0..4   then "less than 5 seconds" + ago
        when 5..9   then "less than 10 seconds" + ago
        when 10..19 then "less than 20 seconds" + ago
        when 20..39 then "half a minute" + ago
        when 40..59 then "less than 1 minute" + ago
        else             "1 minute" + ago
        end
        
      when 2..44           then "#{distance_in_minutes} minutes" + ago
      when 45..89          then "about 1 hour" + ago
      when 90..1439        then "about #{(distance_in_minutes.to_f / 60.0).round} hours" + ago
      when 1440..2529      then "about 1 day" + ago
      when 2530..43199     then "about #{(distance_in_minutes.to_f / 1440.0).round} days" + ago
      when 43200..86399    then "about 1 month" + ago
      when 86400..525599   then "about #{(distance_in_minutes.to_f / 43200.0).round} months" + ago
      else
        distance_in_years           = distance_in_minutes / 525600
        minute_offset_for_leap_year = (distance_in_years / 4) * 1440
        remainder                   = ((distance_in_minutes - minute_offset_for_leap_year) % 525600)
        if remainder < 131400
          "about #{distance_in_years} years" + ago
        elsif remainder < 394200
          "over #{distance_in_years} years" + ago
        else
          "almost #{distance_in_years} years" + ago
        end
      end
    end

    # to make things easier on ourselves
    get "/?" do
      redirect url_path(:overview)
    end

    %w( overview workers ).each do |page|
      get "/#{page}.poll" do
        show_for_polling(page)
      end

      get "/#{page}/:id.poll" do
        show_for_polling(page)
      end
    end

    %w( overview queues working workers key ).each do |page|
      get "/#{page}" do
        show page
      end

      get "/#{page}/:id" do
        show page
      end
    end

    post "/queues/:id/remove" do
      Resque.remove_queue(params[:id])
      redirect u('queues')
    end

    get "/failed" do
      if Resque::Failure.url
        redirect Resque::Failure.url
      else
        show :failed
      end
    end

    post "/failed/clear" do
      Resque::Failure.clear
      redirect u('failed')
    end

    post "/failed/requeue/all" do
      Resque::Failure.count.times do |num|
        Resque::Failure.requeue(num)
      end
      redirect u('failed')
    end

    get "/failed/requeue/:index" do
      Resque::Failure.requeue(params[:index])
      if request.xhr?
        return Resque::Failure.all(params[:index])['retried_at']
      else
        redirect u('failed')
      end
    end

    get "/failed/remove/:index" do
      Resque::Failure.remove(params[:index])
      redirect u('failed')
    end

    get "/stats" do
      redirect url_path("/stats/resque")
    end

    get "/stats/:id" do
      show :stats
    end

    get "/stats/keys/:key" do
      show :stats
    end

    get "/stats.txt" do
      info = Resque.info

      stats = []
      stats << "resque.pending=#{info[:pending]}"
      stats << "resque.processed+=#{info[:processed]}"
      stats << "resque.failed+=#{info[:failed]}"
      stats << "resque.workers=#{info[:workers]}"
      stats << "resque.working=#{info[:working]}"

      Resque.queues.each do |queue|
        stats << "queues.#{queue}=#{Resque.size(queue)}"
      end

      content_type 'text/plain'
      stats.join "\n"
    end

    def resque
      Resque
    end

    def self.tabs
      @tabs ||= ["Overview", "Working", "Failed", "Queues", "Workers", "Stats"]
    end
  end
end

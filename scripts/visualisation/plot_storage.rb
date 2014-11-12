# Plots bandwidth usage chart of Global Storage
#
# Has two modes:
#   - Number mode. Shows number of VMs downloading or uploading files at the same time.
#   - Speed mode. Shows upload/download bandwidth of GS in any point of time.
# 
# Examples of usage:
#   $ ruby plot_storage.rb number preprocessed.log output_graph
#   $ ruby plot_storage.rb number preprocessed.log output_graph --resolution=10000,600
#   $ ruby plot_storage.rb speed preprocessed.log output_graph --crop-from=2.5 --crop-to=4


require 'rubygems'
require 'gnuplot'
require 'set'
require 'main'

require './parsed_log_loader.rb'

def plot_number_schedule(logs, params)
  filename = params['output_filename'].value
  resolution = params['resolution'].value

  Gnuplot.open do |gp|
    Gnuplot::Plot.new( gp ) do |plot|
      plot.xlabel "Time (seconds)"
      plot.ylabel "Number of readers/writers"
      plot.set "key right outside"
      plot.terminal "png size #{resolution}"
      plot.output filename + ".png"

      times = logs.collect { |log| log.time }
      readers_numbers = logs.collect { |log| log.readers_number }
      writers_numbers = logs.collect { |log| log.writers_number }

      readers_series = Gnuplot::DataSet.new( [times, readers_numbers] ) do |ds|
        ds.using = "1:2"
        ds.with = "lines"
        ds.title = "readers"
      end

      writers_series = Gnuplot::DataSet.new( [times, writers_numbers] ) do |ds|
        ds.using = "1:2"
        ds.with = "lines"
        ds.title = "writers"
      end

      plot.data = [readers_series, writers_series]
      
      max_number = (readers_numbers + writers_numbers).max

      ymax = max_number * 1.05
      ymin = max_number - ymax
      plot.yrange "[#{ymin}:#{ymax}]"

      if params['crop-from'].given? and params['crop-to'].given?
        x_min = params['crop-from'].value
        x_max = params['crop-to'].value
        plot.xrange "[#{x_min}:#{x_max}]"
      elsif params['crop-from'].given?
        x_min = params['crop-from'].value
        plot.xrange "[#{x_min}:]"
      elsif params['crop-to'].given?
        x_max = params['crop-to'].value
        plot.xrange "[:#{x_max}]"
      end
    end
  end
end

def plot_bandwidth_schedule(logs, params)
  filename = params['output_filename'].value
  resolution = params['resolution'].value

  Gnuplot.open do |gp|
    Gnuplot::Plot.new( gp ) do |plot|
      plot.xlabel "Time (seconds)"
      plot.ylabel "Bandwidth (bytes/second)"
      plot.set "key right outside"
      plot.terminal "png size #{resolution}"
      plot.output filename + ".png"

      times = logs.collect { |log| log.time }
      read_bandwidths = logs.collect { |log| log.read_speed }
      write_bandwidths = logs.collect { |log| log.write_speed }

      read_series = Gnuplot::DataSet.new( [times, read_bandwidths] ) do |ds|
        ds.using = "1:2"
        ds.with = "lines"
        ds.title = "read"
      end

      write_series = Gnuplot::DataSet.new( [times, write_bandwidths] ) do |ds|
        ds.using = "1:2"
        ds.with = "lines"
        ds.title = "write"
      end

      plot.data = [read_series, write_series]

      max_speed = (read_bandwidths + write_bandwidths).max
      ymax = max_speed * 1.05
      ymin = max_speed - ymax
      plot.yrange "[#{ymin}:#{ymax}]"

      if params['crop-from'].given? and params['crop-to'].given?
        x_min = params['crop-from'].value
        x_max = params['crop-to'].value
        plot.xrange "[#{x_min}:#{x_max}]"
      elsif params['crop-from'].given?
        x_min = params['crop-from'].value
        plot.xrange "[#{x_min}:]"
      elsif params['crop-to'].given?
        x_max = params['crop-to'].value
        plot.xrange "[:#{x_max}]"
      end
    end
  end
end

def load_storage_logs(log_filename)
  logs = read_log_from_file(log_filename)
  storage_logs = logs[:storage_states]

  if storage_logs.empty?
    return []
  end

  refined_storage_logs = []
  for i in 0...storage_logs.length-1
    refined_storage_logs.push(storage_logs[i])
    refined_storage_logs.push(StorageState.new(storage_logs[i+1].time, 
                                       storage_logs[i].readers_number,
                                       storage_logs[i].writers_number,
                                       storage_logs[i].read_speed,
                                       storage_logs[i].write_speed))
  end

  refined_storage_logs.push(storage_logs[-1])
  return refined_storage_logs  
end

Main {
  argument('log_filename') {
    required
    description "Path to log file in intermediate format (preprocessed)."
  }

  argument('output_filename') {
    required
    description "Result image filename. .png extension will be added to this filename."
  }

  option('resolution') {
    argument :required
    description "Resolution of created graph."
    default "1600,900"
    validate { |comma_separated_resolution| /\d+,\d+/ =~ comma_separated_resolution }
  }

  option('crop-from') {
    argument :required
    description "Show only events after given point of time"
    cast :float
  }

  option('crop-to') {
    argument :required
    description "Show only events before given point of time"
    cast :float
  }

  mode 'number' do
    def run() 
      refined_storage_logs = load_storage_logs(params['log_filename'].value)
      if refined_storage_logs.empty?
        print "No storage information. Probably all schedules were rejected or you run simulation with void storage type.\n"
        return
      end
      plot_number_schedule(refined_storage_logs, params)
    end
  end

  mode 'speed' do
    def run() 
      refined_storage_logs = load_storage_logs(params['log_filename'].value)
      if refined_storage_logs.empty?
        print "No storage information. Probably all schedules were rejected or you run simulation with void storage type.\n"
        return
      end
      plot_bandwidth_schedule(refined_storage_logs, params)
    end
  end

  def run()
    print "No mode given (number|speed)!\n"
    return
  end
}

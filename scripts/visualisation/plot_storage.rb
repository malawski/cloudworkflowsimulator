require 'rubygems'
require 'gnuplot'
require 'set'

def read_log(file_content)
  lines = file_content.split(/\n/)

  lines.each do |line| 
    # match = 
  end
end

def read_log_from_file(filename)
  file_content = `cat #{filename}`
  return read_log(file_content)
end

class StorageState
  def initialize(time, readers_number, writers_number, read_speed, write_speed)
    @time = time
    @readers_number = readers_number
    @writers_number = writers_number
    @read_speed = read_speed
    @write_speed = write_speed
  end

  attr_reader :time, :readers_number, :writers_number, :read_speed, :write_speed
end

def plot_number_schedule(logs, filename)
  Gnuplot.open do |gp|
    Gnuplot::Plot.new( gp ) do |plot|
      plot.xlabel "Time"
      plot.ylabel "Read/Write number"
      plot.set "key right outside"
      plot.terminal "png size 1024,768"
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
    end
  end
end

def plot_bandwidth_schedule(logs, filename)
  Gnuplot.open do |gp|
    Gnuplot::Plot.new( gp ) do |plot|
      plot.xlabel "Time"
      plot.ylabel "Bandwidth"
      plot.set "key right outside"
      plot.terminal "png size 1024,768"
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
    end
  end
end


log_filename = ARGV[0]
type = ARGV[1]
output_filename = "storage"

# logs = read_log_from_file(log_filename)
logs = [
    StorageState.new(10.0, 0, 0, 300000, 100000),
    StorageState.new(20.0, 1, 0, 300000, 100000),
    StorageState.new(30.0, 1, 1, 300000, 100000),
    StorageState.new(50.0, 2, 1, 150000, 100000),
    StorageState.new(69.0, 2, 0, 150000, 100000),
    StorageState.new(89.0, 1, 0, 300000, 100000),
    StorageState.new(99.0, 0, 0, 300000, 100000)]

refined_logs = []

for i in 0...logs.length-1
  refined_logs.push(logs[i])
  refined_logs.push(StorageState.new(logs[i+1].time, logs[i].readers_number, logs[i].writers_number,
                                     logs[i].read_speed, logs[i].write_speed))
end

refined_logs.push(logs[-1])

case type
when "number"
  plot_number_schedule(refined_logs, output_filename)
when "speed"
  plot_bandwidth_schedule(refined_logs, output_filename)
end




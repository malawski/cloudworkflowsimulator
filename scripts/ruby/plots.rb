require 'scanf.rb'
require 'rubygems'
require 'gnuplot'
require 'set'


# Plot Gantt charts from CloudSim log files using Gnuplot
# 
# Author: Maciej Malawski


class Tasks
  attr_reader :xlo, :xhi, :y, :ids, :types, :distinct_types
  
  def read_tasks (filename)
    
    str = `cat #{filename}.txt | grep SUCCESS`

    jobs = Hash.new
    @distinct_types = Set.new

    str.each do |line|
      data = line.scanf(" %d   SUCCESS  %d %d %f %f %f")
      jobs[data[0]] = [data [1], data[2], data[4], data[5]]
    end

    @xlo = Array.new
    @xhi = Array.new
    @y = Array.new
    @ids = Array.new
    @types = Array.new

    jobs.keys.each do |id|
      @xlo[id] = jobs[id][2]
      @xhi[id] = jobs[id][3]
      @y[id] = jobs[id][1]
      @ids[id] = id
      @types[id] = jobs[id][0]
      @distinct_types.add(jobs[id][0])
    end
  end
  
  
end


def read_array (filter)
  str = `#{filter}`

  cols = Array.new

  str.each do |line|
    row = line.split
    if cols[0]==nil
      # create array of columns
      for i in 0..row.size-1
        cols[i]=Array.new
      end
    end
    for i in 0..row.size-1
        cols[i].push row[i]
    end
  end
  return cols
end
  


def plot_schedule (filename)

  tasks = Tasks.new
  tasks.read_tasks(filename)
  
  vms = read_array("cat #{filename}-vms.txt | grep -e '[0-9]'  ")

  ymax = vms.length + 0.8
  
  inputs = read_array("cat #{filename}-inputs-transfer.txt | grep -e '[0-9]'  ")

  outputs = read_array("cat #{filename}-outputs-transfer.txt | grep -e '[0-9]'  ")
    
  

  Gnuplot.open do |gp|
    Gnuplot::Plot.new( gp ) do |plot|

      plot.title  "Schedule " + File.basename(filename)
      plot.xlabel "time"
      plot.ylabel "VM ID"
      plot.xtics 3600
      #plot.ytics 50
      #plot.yrange "[-0.8:#{ymax}]"
      #plot.terminal 'pdf size 8.5,11 font "arial,6" linewidth 1'
      #plot.terminal 'pdf size 11,8.5 font "arial,6" linewidth 1'
      #plot.output filename + ".pdf"
      #puts "Saving plot to file: " + filename + ".pdf"
      plot.set "key right outside"
      plot.set "grid"
      #plot.terminal "png size 1024,768"
      #plot.output 'output/' + dag + deadline + ".png"
      #plot.output filename + ".png"

      data = Array.new

      vmset = Gnuplot::DataSet.new( vms ) do |ds|
        ds.using = "2:1:2:3:($1-0.5):($1+0.5)"
        ds.with = "boxxyerrorbars fs solid 0.55 noborder"
        ds.title = 'VMs'
      end
      
      data.push(vmset) if vms.size > 0
      
      inputset = Gnuplot::DataSet.new( inputs ) do |ds|
        ds.using = "2:1:2:3:($1-0.3):($1+0.3)"
        ds.with = "boxxyerrorbars fs solid 0.55 "
        ds.title = 'Inputs'
      end
      
      data.push(inputset) if inputs.size > 0

      outputset = Gnuplot::DataSet.new( outputs ) do |ds|
        ds.using = "2:1:2:3:($1-0.3):($1+0.3)"
        ds.with = "boxxyerrorbars fs solid 0.55 "
        ds.title = 'Outputs'
      end
      
      data.push(outputset) if outputs.size > 0
      
            
      tasks.distinct_types.to_a.sort.each do |type|
        # here we do filtering based on type (e.g. priority)
        dataset = Gnuplot::DataSet.new( [tasks.xlo, tasks.y, tasks.xlo, tasks.xhi, tasks.types] ) do |ds|
          ds.using = "($5==#{type} ? $1 : NaN):2:3:4:($2-0.4):($2+0.4)"
          ds.with = "boxxyerrorbars fs solid 0.55"
          ds.title = "Job type #{type}"
        end
        data.push(dataset)
      end

                    
      plot.data = data
      
    end

  end

end
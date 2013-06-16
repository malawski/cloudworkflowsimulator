require 'scanf.rb'
require 'rubygems'
require 'gnuplot'
require 'set'


# Plot Gantt charts from CloudSim log files using Gnuplot
# 
# Author: Maciej Malawski


class Tasks
  attr_reader :task_start_time, :computation_start_time, :computation_finish_time, :task_finish_time, :y, :ids, :types, :distinct_types
  
  def read_tasks (filename)
    
    str = `cat #{filename}.txt`

    jobs = Hash.new
    @distinct_types = Set.new

    str.each do |line|
      data = line.scanf("%d %d %f %f %f %f")
      jobId = data[0]
      jobs[jobId] = [data[1], data[2], data[3], data[4], data[5]]
    end

    @task_start_time = Array.new
    @computation_start_time = Array.new
    @computation_finish_time = Array.new
    @task_finish_time = Array.new
    @y = Array.new
    @ids = Array.new
    @types = Array.new

    jobs.keys.each do |id|
      @task_start_time[id] = jobs[id][1]
      @computation_start_time[id] = jobs[id][2]
      @computation_finish_time[id] = jobs[id][3]
      @task_finish_time[id] = jobs[id][4]
      @y[id] = jobs[id][0]
      @ids[id] = id
      #@types[id] = jobs[id][0]
      #@distinct_types.add(jobs[id][0])
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
  
  # change this to plot VMs
  # vms = read_array("cat #{filename}-vms.txt | grep -e '[0-9]'  ")
  vms = {}
  
  ymax = vms.length + 0.8
  
  #inputs = read_array("cat #{filename}-inputs-transfer.txt | grep -e '[0-9]'  ")

  #outputs = read_array("cat #{filename}-outputs-transfer.txt | grep -e '[0-9]'  ")
    


  Gnuplot.open do |gp|
    Gnuplot::Plot.new( gp ) do |plot|

      #plot.title  "Schedule " + File.basename(filename)
      plot.xlabel "Time"
      plot.ylabel "VM"
      #plot.xtics 3600
      plot.ytics 1
      #plot.yrange "[-0.8:#{ymax}]"
      #plot.terminal 'pdfcairo size 5,1.5 font "arial,8" linewidth 1'
      #plot.terminal 'pdf size 11,8.5 font "arial,6" linewidth 1'
      #plot.output filename + ".pdf"
      #puts "Saving plot to file: " + filename + ".pdf"
      #plot.set "key right outside"
      plot.set "key off"
      #plot.noytics
      #plot.noxtics
      #plot.set "grid"
      plot.set "style line 1 lc rgb 'grey10' lt 1 lw 1"
      plot.set "style line 2 lc rgb 'brown' lt 1 lw 1"
      plot.set "style line 3 lc rgb 'orange' lt 1 lw 1"
      plot.set "style line 4 lc rgb 'blue' lt 1 lw 1"
      plot.set "style line 5 lc rgb 'green' lt 1 lw 1"
      plot.set "style line 6 lc rgb 'grey10' lt 1 lw 1"
      plot.set "style line 7 lc rgb 'brown' lt 1 lw 1"
      plot.set "style line 8 lc rgb 'orange' lt 1 lw 1"
      plot.set "style line 9 lc rgb 'blue' lt 1 lw 1"
      plot.set "style line 10 lc rgb 'green' lt 1 lw 1"
      plot.set "style fill border lc rgb 'black'"
      plot.terminal "png size 1024,768"
      #plot.output 'output/' + dag + deadline + ".png"
      plot.output filename + ".png"

      data = Array.new

      #vmset = Gnuplot::DataSet.new( vms ) do |ds|
        #ds.using = "2:1:2:3:($1-0.5):($1+0.5)"
        #ds.with = "boxxyerrorbars fs solid 0.55 noborder"
        #ds.title = 'VMs'
      #end
      
      #data.push(vmset) if vms.size > 0
      
      #inputset = Gnuplot::DataSet.new( inputs ) do |ds|
      #  ds.using = "2:1:2:3:($1-0.3):($1+0.3)"
      #  ds.with = "boxxyerrorbars fs solid 0.55 "
      #  ds.title = 'Inputs'
      #end
      
      #data.push(inputset) if inputs.size > 0

      #outputset = Gnuplot::DataSet.new( outputs ) do |ds|
      #  ds.using = "2:1:2:3:($1-0.3):($1+0.3)"
      #  ds.with = "boxxyerrorbars fs solid 0.55 "
      #  ds.title = 'Outputs'
      #end
      
      #data.push(outputset) if outputs.size > 0
      
            
      #tasks.distinct_types.to_a.sort.each do |type|
        # puts type
        # here we do filtering based on type (e.g. priority)



      input_makespans = Gnuplot::DataSet.new( [tasks.task_start_time, tasks.y, tasks.task_start_time, tasks.computation_start_time] ) do |ds|
        ds.using = "($1):2:3:4:($2-0.4):($2+0.4)"
        ds.with = "boxxyerrorbars fs solid 0.55"
      end

      computational_makespans = Gnuplot::DataSet.new( [tasks.computation_start_time, tasks.y, tasks.computation_start_time, tasks.computation_finish_time] ) do |ds|
        ds.using = "($1):2:3:4:($2-0.4):($2+0.4)"
        ds.with = "boxxyerrorbars fs solid 0.55"
      end

      output_makespans = Gnuplot::DataSet.new( [tasks.computation_finish_time, tasks.y, tasks.computation_finish_time, tasks.task_finish_time] ) do |ds|
        ds.using = "($1):2:3:4:($2-0.4):($2+0.4)"
        ds.with = "boxxyerrorbars fs solid 0.55"
      end

      data.push(input_makespans)
      data.push(computational_makespans)
      data.push(output_makespans)

      plot.data = data
      
    end

  end

end
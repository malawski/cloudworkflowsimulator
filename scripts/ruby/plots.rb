require 'scanf.rb'
require 'rubygems'
require 'gnuplot'
require 'set'

# Plot Gantt charts from CloudSim log files using Gnuplot
#
# Script needs input in format [jobId, VM, inputsStartTime, computationalTaskStartTime, computationalTaskEndTime, outputsEndTime]:
#
# Example:
#    00007 12 31.6 32.5 38.9 40.9
#    00017 4 29.3 30.1 32.8 38.9
#
# This format is consistent with output of script prepare_data_for_gantt.py which converts cloudsim log to desired input format

class Tasks
  attr_reader :task_start_time, :computation_start_time, :computation_finish_time, :task_finish_time, :vm, :ids, :types, :distinct_types
  
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
    @vm = Array.new
    @ids = Array.new
    @types = Array.new

    jobs.keys.each do |id|
      @task_start_time[id] = jobs[id][1]
      @computation_start_time[id] = jobs[id][2]
      @computation_finish_time[id] = jobs[id][3]
      @task_finish_time[id] = jobs[id][4]
      @vm[id] = jobs[id][0]
      @ids[id] = id
      #@types[id] = jobs[id][0]
      #@distinct_types.add(jobs[id][0])
    end
  end
end

def createGanttSeries (startsList, finishesList, rows)
  return Gnuplot::DataSet.new( [startsList, rows, startsList, finishesList] ) do |ds|
    ds.using = "($1):2:3:4:($2-0.4):($2+0.4)"
    ds.with = "boxxyerrorbars fs solid 0.55"
  end
end

def plot_schedule (filename)
  tasks = Tasks.new
  tasks.read_tasks(filename)

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
      plot.output filename + ".png"

      data = Array.new

      input_makespans = createGanttSeries(tasks.task_start_time, tasks.computation_start_time, tasks.vm)
      computational_makespans = createGanttSeries(tasks.computation_start_time, tasks.computation_finish_time, tasks.vm)
      output_makespans = createGanttSeries(tasks.computation_finish_time, tasks.task_finish_time, tasks.vm)

      data.push(input_makespans)
      data.push(computational_makespans)
      data.push(output_makespans)

      plot.data = data

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
    end
  end
end
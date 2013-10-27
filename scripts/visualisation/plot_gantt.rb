# TODO(mequrel): add description

require 'scanf.rb'
require 'rubygems'
require 'gnuplot'
require 'set'

class TaskLog
  def initialize(workflow, id, vm, started, finished, result)
    @workflow = workflow
    @id = id
    @vm = vm
    @started = started
    @finished = finished
    @result = result
  end

  attr_reader :workflow, :id, :vm, :started, :finished, :result
end

class VMLog
  def initialize(id, started, finished)
    @id = id
    @started = started
    @finished = finished
  end

  attr_reader :id, :started, :finished
end

class Workflow
  def initialize(id, priority)
    @id = id
    @priority = priority
  end

  attr_reader :id, :priority
end

def read_log(file_content)
  lines = file_content.split(/\n/)
  current_line = 0

  vm_number = lines[current_line].to_i
  current_line += 1

  vms = Hash.new

  for i in 0...vm_number
    vm_info = lines[current_line].split

    vm = VMLog.new(vm_info[0], vm_info[1].to_f, vm_info[2].to_f)
    vms[vm.id] = vm

    current_line += 1
  end

  workflows_number = lines[current_line].to_i
  current_line += 1

  workflows = Hash.new

  for i in 0...workflows_number
    workflow_info = lines[current_line].split
    workflow = Workflow.new(workflow_info[0], workflow_info[1].to_i)
    workflows[workflow.id] = workflow
    current_line += 1
  end

  tasks_number = lines[current_line].to_i
  current_line += 1

  tasks = []

  for i in 0...tasks_number
    task_info = lines[current_line].split
    task = TaskLog.new(task_info[0], task_info[1], task_info[2], task_info[3].to_f, task_info[4].to_f, task_info[5])
    tasks.push(task)
    current_line += 1
  end

  return {
    :vms => vms,
    :workflows => workflows,
    :tasks => tasks
  }
end

def read_log_from_file(filename)
  file_content = `cat #{filename}`
  return read_log(file_content)
end

def createGanttSeries (startsList, finishesList, rows)
  return Gnuplot::DataSet.new( [startsList, rows, startsList, finishesList] ) do |ds|
    ds.using = "($1):2:3:4:($2-0.4):($2+0.4)"
    ds.with = "boxxyerrorbars fs solid 0.55"
  end
end

def plot_schedule (logs, filename)
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

      data = []

      tasks = logs[:tasks]

      started_row = tasks.collect { |task| task.started }
      finished_row = tasks.collect { |task| task.finished }
      vm_row = tasks.collect { |task| task.vm[2].to_i }
      puts vm_row

      makespans = createGanttSeries(started_row, finished_row, vm_row)

      data.push(makespans)

      # input_makespans = createGanttSeries(tasks.task_start_time, tasks.computation_start_time, tasks.vm)
      # computational_makespans = createGanttSeries(tasks.computation_start_time, tasks.computation_finish_time, tasks.vm)
      # output_makespans = createGanttSeries(tasks.computation_finish_time, tasks.task_finish_time, tasks.vm)

      # data.push(input_makespans)
      # data.push(computational_makespans)
      # data.push(output_makespans)

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

logs = read_log_from_file(ARGV[0])
plot_schedule(logs, "test")

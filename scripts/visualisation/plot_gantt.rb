# Plots gantt charts for given scheduling logs.
#
# Has three modes:
#   - Result mode. Useful to distinguish failed and retried tasks.
#   - Workflow mode. Useful to distinguish tasks from different workflows.
#   - Storage mode. Useful to distinguish upload, download and computational part of task.
# 
# Examples of usage:
#   $ ruby plot_gantt.rb results tests/test1.log output_graph
#   $ ruby plot_gantt.rb workflow tests/test1.log output_graph --resolution=10000,600
#   $ ruby plot_gantt.rb storage tests/test2.log output_graph

require 'scanf.rb'
require 'rubygems'
require 'gnuplot'
require 'set'
require 'main'

require 'parsed_log_loader.rb'

class GanttPlotter
  def initialize()
    @data = []
    @colors = {
      :red => 'red',
      :blue => 'grey90',
      :green => 'green',
      :orange => 'orange',
      :brown => 'brown', 
      :dark_grey => 'grey10'
    }
    @types = {
      :dotted => 0,
      :straight => 1
    }
    @styles = {}
    @style_next_id = 1
  end

  def create_gantt_series (startsList, finishesList, rows, line_style, title)
    return Gnuplot::DataSet.new( [startsList, rows, startsList, finishesList] ) do |ds|
      ds.using = "($1):2:3:4:($2-0.4):($2+0.4):yticlabels(2) ls #{line_style}"
      ds.with = "boxxyerrorbars fs solid 0.55"
      ds.title = title
    end
  end

  def get_line_style(color, type)
    return @styles[[color, type]]
  end

  def add_series(series, title, color, type=:straight)
    vm_row = series[:vms].collect { |vm| vm.sub("VM", "").to_i }
    started_row = series[:started]
    finished_row = series[:finished]

    if vm_row.empty?
      return
    end

    add_style_line_if_not_exist color, type
    line_style = get_line_style color, type

    makespans = create_gantt_series(started_row, finished_row, vm_row, line_style, title)
    @data.push(makespans)
  end

  def add_style_line_if_not_exist(color, type)
    key = [color, type]
    if not @styles.has_key? key
      @styles[key] = @style_next_id
      @style_next_id += 1
    end
  end

  def plot(params)
    filename = params['output_filename'].value
    resolution = params['resolution'].value

    Gnuplot.open do |gp|
      Gnuplot::Plot.new( gp ) do |plot|
        @styles.each do |key, style_id|
          color, type = key
          gp_type = @types[type]
          gp_color = @colors[color]
          plot.set "style line #{style_id} lc rgb '#{gp_color}' lt #{gp_type} lw 1"
        end

        plot.set "style fill"
        plot.xlabel "Time"
        plot.ylabel "VM"
        plot.ytics 1
        plot.set "key right outside"
        plot.terminal "png size #{resolution}"
        plot.output filename + ".png"

        plot.data = @data
      end
    end
  end
end

def get_task_series(tasks)
  return {
    :vms => tasks.collect { |task| task.vm },
    :started => tasks.collect { |task| task.started },
    :finished => tasks.collect { |task| task.finished }
  }
end

def plot_result_schedule (logs, params)
  plotter = GanttPlotter.new

  vms = logs[:vms].values
  provisioning_series = {
    :vms => vms.collect { |vm| vm.id },
    :started => vms.collect { |vm| vm.started },
    :finished => vms.collect { |vm| vm.finished }
  }
  plotter.add_series provisioning_series, "VM idle", :blue
 
  tasks = logs[:tasks]

  finished_tasks = tasks.select { |task| task.result.include? "OK" and not task.result.include? "RETRY" }
  plotter.add_series get_task_series(finished_tasks), "Done", :green

  failed_tasks = tasks.select { |task| task.result.include? "FAILED" and not task.result.include? "RETRY"  }
  plotter.add_series get_task_series(failed_tasks), "Failed", :red

  retried_finished_tasks = tasks.select { |task| task.result.include? "OK" and task.result.include? "RETRY" }
  plotter.add_series get_task_series(retried_finished_tasks), "Retry", :green, :dotted

  retried_failed_tasks = tasks.select { |task| task.result.include? "FAILED" and task.result.include? "RETRY" }
  plotter.add_series get_task_series(retried_failed_tasks), "Retry failed", :red, :dotted

  plotter.plot(params)
end

def plot_workflow_schedule(logs, params)
  plotter = GanttPlotter.new

  vms = logs[:vms].values
  provisioning_series = {
    :vms => vms.collect { |vm| vm.id },
    :started => vms.collect { |vm| vm.started },
    :finished => vms.collect { |vm| vm.finished }
  }
  plotter.add_series provisioning_series, "VM idle", :blue
 
  tasks = logs[:tasks]
  tasks_by_workflow = tasks.group_by { |task| task.workflow }

  # TODO(mequrel): sort by priorities

  workflows = logs[:workflows].values

  colors = [:red, :green, :orange, :dark_grey, :brown]

  workflows.reverse.each_with_index do |workflow, i|
    color = colors[i % colors.length]
    workflow_tasks = tasks_by_workflow[workflow.id]
    plotter.add_series get_task_series(workflow_tasks), "#{workflow.id} (#{workflow.priority})" , color
  end

  plotter.plot(params)
end

def plot_storage_schedule(logs, params)
  plotter = GanttPlotter.new

  vms = logs[:vms].values
  provisioning_series = {
    :vms => vms.collect { |vm| vm.id },
    :started => vms.collect { |vm| vm.started },
    :finished => vms.collect { |vm| vm.finished }
  }
  plotter.add_series provisioning_series, "VM idle", :blue
 
  tasks = logs[:tasks]
  plotter.add_series get_task_series(tasks), "Computation", :dark_grey

  transfers = logs[:transfers]
  input_transfers = transfers.select { |transfer| transfer.direction == "UPLOAD" }
  plotter.add_series get_task_series(input_transfers), "Upload", :orange

  output_transfers = transfers.select { |transfer| transfer.direction == "DOWNLOAD" }
  plotter.add_series get_task_series(output_transfers), "Download", :green

  plotter.plot(params)
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
    default "1024,768"
    validate { |comma_separated_resolution| /\d+,\d+/ =~ comma_separated_resolution }
  }

  mode 'results' do
    def run() 
      logs = read_log_from_file(params['log_filename'].value)
      plot_result_schedule(logs, params)
    end
  end

  mode 'workflows' do
    def run() 
      logs = read_log_from_file(params['log_filename'].value)
      plot_workflow_schedule(logs, params)
    end
  end

  mode 'storage' do 
    def run()
      logs = read_log_from_file(params['log_filename'].value)
      plot_storage_schedule(logs, params)
    end
  end

  def run()
    print "No mode given!\n\n"
    help!
  end
}
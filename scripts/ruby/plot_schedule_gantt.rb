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
      #plot.ytics 50
      #plot.yrange "[-0.8:#{ymax}]"
      #plot.terminal 'pdf size 8.5,11 font "arial,6" linewidth 1'
      #plot.terminal 'pdf size 11,8.5 font "arial,6" linewidth 1'
      #plot.output filename + ".pdf"
      #puts "Saving plot to file: " + filename + ".pdf"

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


#plot_schedule 'testDatacenterCloudlets2'
#plot_schedule 'testDatacenterDAG'
#plot_schedule 'testDatacenterDAG2'
#plot_schedule 'testDatacenterReadDAG'
#plot_schedule 'testDatacenterProvisionerDAG'
#plot_schedule 'testDatacenterDeprovisionerDAG'
#plot_schedule 'testDatacenterDeprovisioner30DAG'
#plot_schedule 'testDatacenterDeprovisioner50DAG'
#plot_schedule 'testDatacenterDeprovisioner100DAG'
#plot_schedule 'output/testDatacenterDeprovisioner1000DAG'
#plot_schedule 'output/DeprovisionerCyberShake_100'
#plot_schedule 'output/Deprovisionercybershake_small'
#plot_schedule 'output/EnsembleCyberShake_30_50'
#plot_schedule 'output/EnsembleCyberShake_10x100'
#plot_schedule 'output/CyberShake_100.dag10x20'
#plot_schedule 'output/CyberShake_100.dag20x10'
#plot_schedule 'output/CyberShake_100.dag20x9'
#plot_schedule 'output/CyberShake_1000.dag20x10'
#plot_schedule 'output/CyberShake_1000.dag200x10'
#plot_schedule 'output/CyberShake_1000.dag2000x10'
#plot_schedule 'output/CyberShake_1000.dag800x10'
#plot_schedule 'output/cybershake_small.dag256x10'
#plot_schedule 'output/cybershake_small.dag2560x10'
#plot_schedule 'output/cybershake_small.dag256x20'
#plot_schedule 'output/TransferCyberShake_30'
#plot_schedule 'output/TransferInOutCyberShake_30'
#plot_schedule 'output/TransferInOutCyberShake_1000'
#plot_schedule 'output/CacheInOutCyberShake_1000'
#plot_schedule 'output/MinMinInOutCyberShake_1000'
#plot_schedule 'output/CyberShake_1000'
#plot_schedule 'output/MinMinCyberShake_1000'
#plot_schedule 'output/MaxMinCyberShake_1000'
#plot_schedule 'output/cybershake_small'
#plot_schedule 'output/MinMincybershake_small'
#plot_schedule 'output/MaxMincybershake_small'
#plot_schedule 'output/Planner-CyberShake_30'
#plot_schedule 'output/CyberShake_30.dag10x1'

#plot_schedule 'output/CyberShake_30.dag10x10'
#plot_schedule 'output/Planner-CyberShake_30x10'
#plot_schedule 'output/PlannerDeadline500-CyberShake_30'
#plot_schedule 'output/PlannerDeadline1000x10-CyberShake_30'
#plot_schedule 'output/PlannerDeadline800x10-CyberShake_30'
#plot_schedule 'output/PlannerDeadline700x2-CyberShake_30'
#plot_schedule 'output/PlannerDeadline500x9-CyberShake_30'


#plot_schedule 'output/testScheduleDag'
#plot_schedule 'output/testScheduleDag_CyberShake_100' 

#plot_schedule 'output/testDynamicSchedulerDag'
#plot_schedule 'output/testDynamicSchedulerDag_CyberShake_100' 

#plot_schedule 'output/testEnsembleDynamicSchedulerDag_CyberShake_100x10'
#plot_schedule 'output/testEnsembleProvisionerDynamicSchedulerDag_CyberShake_100x10'
#plot_schedule 'output/testEnsembleUtilizationProvisionerDynamicSchedulerDag_CyberShake_100x10'


plot_schedule 'output/testSimpleUtilizationBasedProvisionerEnsembleDynamicSchedulerCyberShake_1000.dagx40d7200.0b45.0'
plot_schedule 'output/testSimpleUtilizationBasedProvisionerEnsembleDynamicSchedulerCyberShake_1000.dagx40d7200.0b44.0'
plot_schedule 'output/testSimpleUtilizationBasedProvisionerEnsembleDynamicSchedulerCyberShake_1000.dagx40d7200.0b10.0'
plot_schedule 'output/testSimpleUtilizationBasedProvisionerEnsembleDynamicSchedulerCyberShake_1000.dagx40d7200.0b9.0'
plot_schedule 'output/testSimpleUtilizationBasedProvisionerEnsembleDynamicSchedulerCyberShake_1000.dagx40d7200.0b8.0'
plot_schedule 'output/testSimpleUtilizationBasedProvisionerEnsembleDynamicSchedulerCyberShake_1000.dagx40d7200.0b7.0'
plot_schedule 'output/testSimpleUtilizationBasedProvisionerEnsembleDynamicSchedulerCyberShake_1000.dagx40d7200.0b6.0'
plot_schedule 'output/testSimpleUtilizationBasedProvisionerEnsembleDynamicSchedulerCyberShake_1000.dagx40d7200.0b5.0'
plot_schedule 'output/testSimpleUtilizationBasedProvisionerEnsembleDynamicSchedulerCyberShake_1000.dagx40d7200.0b4.0'
plot_schedule 'output/testSimpleUtilizationBasedProvisionerEnsembleDynamicSchedulerCyberShake_1000.dagx40d7200.0b1.0'
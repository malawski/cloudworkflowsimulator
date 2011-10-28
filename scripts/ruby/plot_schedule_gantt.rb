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


def plot_schedule (filename)

  tasks = Tasks.new
  tasks.read_tasks(filename)

  str = `cat #{filename}-vms.txt | grep -e '[0-9]'  `

  vms = Hash.new

  str.each do |line|
    data = line.scanf(" %d %f %f")
    vms[data[0]] = [data [1], data[2]]
  end

  vmxlo = Array.new
  vmxhi = Array.new
  vmy = Array.new
  vmids = Array.new

  vms.keys.each do |id|
    vmxlo[id] = vms[id][0]
    vmxhi[id] = vms[id][1]
    vmy[id] = id
    vmids[id] = id
  end

  ymax = vmy.length + 0.8
  
  #puts "Saving plot to file: " + filename + ".pdf"

  Gnuplot.open do |gp|
    Gnuplot::Plot.new( gp ) do |plot|

      plot.title  "Schedule " + File.basename(filename)
      plot.xlabel "time"
      plot.ylabel "VM ID"
      #plot.ytics 50
      #plot.yrange "[-0.8:#{ymax}]"
      #plot.terminal 'pdf size 8.5,11 font "arial,6" linewidth 1'
      #plot.output filename + ".pdf"

      data = Array.new
      
      vmset = Gnuplot::DataSet.new( [vmxlo, vmy, vmxlo, vmxhi] ) do |ds|
        ds.using = "1:2:3:4:($2-0.5):($2+0.5)"
        ds.with = "boxxyerrorbars fs solid 0.55 noborder"
        ds.title = 'VMs'
      end
      
      data.push(vmset)
      
      tasks.distinct_types.to_a.sort.each do |type|
        # here we do filtering based on type (e.g. priority)
        dataset = Gnuplot::DataSet.new( [tasks.xlo, tasks.y, tasks.xlo, tasks.xhi, tasks.types] ) do |ds|
          ds.using = "($5==#{type} ? $1 : NaN):2:3:4:($2-0.4):($2+0.4)"
          ds.with = "boxxyerrorbars fs solid 0.55"
          ds.title = "#{type}"
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
plot_schedule 'output/CyberShake_1000.dag200x10'
#plot_schedule 'output/cybershake_small.dag256x10'
#plot_schedule 'output/cybershake_small.dag2560x10'

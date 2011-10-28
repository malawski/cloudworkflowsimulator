require 'scanf.rb'
require 'rubygems'
require 'gnuplot'


# Plot Gantt charts from CloudSim log files using Gnuplot
# 
# Author: Maciej Malawski


def plot_schedule (filename)

  str = `cat #{filename}.txt | grep SUCCESS`

  cloudlets = Hash.new

  str.each do |line|
    data = line.scanf(" %d   SUCCESS  %d %d %f %f %f")
    cloudlets[data[0]] = [data [1], data[2], data[4], data[5]]
  end

  xlo = Array.new
  xhi = Array.new
  y = Array.new
  ids = Array.new

  cloudlets.keys.each do |id|
    xlo[id] = cloudlets[id][2]
    xhi[id] = cloudlets[id][3]
    y[id] = cloudlets[id][1]
    ids[id] = id
  end

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
  
  puts "Saving plot to file: " + filename + ".pdf"

  Gnuplot.open do |gp|
    Gnuplot::Plot.new( gp ) do |plot|

      plot.title  "Schedule " + File.basename(filename)
      plot.xlabel "time"
      plot.ylabel "VM ID"
      plot.ytics 50
      #plot.yrange "[-0.8:#{ymax}]"
      plot.terminal 'pdf size 8.5,11 font "arial,6" linewidth 1'
      plot.output filename + ".pdf"

      plot.data = [
        Gnuplot::DataSet.new( [vmxlo, vmy, vmxlo, vmxhi] ) do |ds|
        ds.using = "1:2:3:4:($2-0.5):($2+0.5)"
        ds.with = "boxxyerrorbars fs solid 0.55 noborder"
        ds.title = 'VMs'
        end,

        Gnuplot::DataSet.new( [xlo, y, xlo, xhi] ) do |ds|
        ds.using = "1:2:3:4:($2-0.4):($2+0.4)"
        ds.with = "boxxyerrorbars fs solid 0.55 noborder"
        ds.title = 'tasks'
        end
      ]
      
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
plot_schedule 'output/Deprovisionercybershake_small'

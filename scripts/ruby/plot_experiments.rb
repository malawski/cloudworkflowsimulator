require "#{File.dirname(__FILE__)}/plots.rb"

def plot_experiment (f, fA)
  
  Gnuplot.open do |gp|
    Gnuplot::Plot.new( gp ) do |plot|

      plot.title  File.basename(f)
      plot.xlabel "deadline"
      plot.ylabel "# finished DAGs"
      #plot.xtics 10
      #plot.ytics 50
      #plot.yrange "[-0.8:#{ymax}]"
      #plot.terminal 'pdf size 8.5,11 font "arial,6" linewidth 1'
      #plot.terminal 'pdf size 11,8.5 font "arial,6" linewidth 1'
      #plot.output f + ".pdf"
      #puts "Saving plot to file: " + filename + ".pdf"



      dpds_data = Gnuplot::DataSet.new( "'#{f}'" ) do |ds|
        ds.with = "linespoints"
        ds.title = 'DPDS'
        ds.using = "($1/3600):2"
      end
      
      plot.data << dpds_data
      
      adpds_data = Gnuplot::DataSet.new( "'#{fA}'" ) do |ds|
        ds.with = "linespoints"
        ds.title = 'A-DPDS'
        ds.using = "($1/3600):2"
      end
      
      plot.data << adpds_data
      
    end
  end
end

def plot_series (dag, budgets, deadline, max_scaling)
  
  Gnuplot.open do |gp|
    Gnuplot::Plot.new( gp ) do |plot|

      plot.title  dag + " max scaling: " + max_scaling.to_s
      plot.xlabel "deadline in hours"
      plot.ylabel "# finished DAGs"
      plot.set "key right outside"
      plot.set "grid"
      plot.terminal "png size 1024,768"
      plot.output 'output/' + dag + 'h' + deadline + 'm' + max_scaling.to_s + ".png"
      
      budgets.each { | budget |
             
        fA = 'output/' + dag + 'b' + budget.to_s + 'h' + deadline + 'm' + max_scaling.to_s + '-outputAware.txt'
        adpds_data = Gnuplot::DataSet.new( "'#{fA}'" ) do |ds|
          ds.with = "linespoints ls 3"
          ds.title = 'A-DPDS $'  + budget.to_s
          ds.using = "($1/3600):2"
        end
        plot.data << adpds_data       
        
        f = 'output/' + dag + 'b' + budget.to_s + 'h' + deadline + 'm' + max_scaling.to_s + '-outputSimple.txt'
        dpds_data = Gnuplot::DataSet.new( "'#{f}'" ) do |ds|
          ds.with = "linespoints ls 1"
          ds.title = 'DPDS $' + budget.to_s
          ds.using = "($1/3600):2"
        end
        plot.data << dpds_data
           
      }
    end
  end
end

#plot_series('Montage_1000.dag', [400.0, 320.0, 240.0, 160.0, 80.0], '1-20', 0.0)
#plot_series('CyberShake_1000.dag', [400.0, 320.0, 240.0, 160.0, 80.0], '1-20', 0.0)
#plot_series('Inspiral_1000.dag', [2000.0, 1600.0, 1200.0, 800.0, 400.0], '1-40', 0.0)
#plot_series('Epigenomics_997.dag', [40000.0, 32000.0, 24000.0, 16000.0, 8000.0], '10-1500', 0.0)
#plot_series('Sipht_1000.dag', [1000.0, 800.0, 600.0, 400.0, 200.0], '1-50', 0.0)
plot_series('psload_large.dag', [1000.0, 800.0, 600.0, 400.0, 200.0], '1-30', 0.0)
plot_series('psload_medium.dag', [100.0, 80.0, 60.0, 40.0, 20.0], '1-50', 0.0)
#plot_series('psmerge_small.dag', [10000.0, 8000.0, 6000.0, 4000.0, 2000.0], '5-150', 0.0)

#plot_series('Montage_1000.dag', [400.0, 320.0, 240.0, 160.0, 80.0], '1-20', 2.0)
#plot_series('CyberShake_1000.dag', [400.0, 320.0, 240.0, 160.0, 80.0], '1-20', 2.0)
#plot_series('Inspiral_1000.dag', [2000.0, 1600.0, 1200.0, 800.0, 400.0], '1-40', 2.0)
#plot_series('Epigenomics_997.dag', [40000.0, 32000.0, 24000.0, 16000.0, 8000.0], '10-1500', 2.0)
plot_series('Sipht_1000.dag', [1000.0, 800.0, 600.0, 400.0, 200.0], '1-50', 2.0)
plot_series('psload_large.dag', [1000.0, 800.0, 600.0, 400.0, 200.0], '1-30', 2.0)
plot_series('psload_medium.dag', [100.0, 80.0, 60.0, 40.0, 20.0], '1-50', 2.0)
#plot_series('psmerge_small.dag', [10000.0, 8000.0, 6000.0, 4000.0, 2000.0], '5-150', 2.0)



#plot_experiment('output/Montage_1000.dagb40.0h1-20-outputSimple.txt','output/Montage_1000.dagb40.0h1-20-outputAware.txt')
#plot_experiment('output/Montage_1000.dagb80.0h1-20-outputSimple.txt','output/Montage_1000.dagb80.0h1-20-outputAware.txt')
#plot_experiment('output/Montage_1000.dagb120.0h1-20-outputSimple.txt','output/Montage_1000.dagb120.0h1-20-outputAware.txt')
#plot_experiment('output/Montage_1000.dagb160.0h1-20-outputSimple.txt','output/Montage_1000.dagb160.0h1-20-outputAware.txt')
#plot_experiment('output/Montage_1000.dagb200.0h1-20-outputSimple.txt','output/Montage_1000.dagb200.0h1-20-outputAware.txt')
#
#plot_experiment('output/CyberShake_1000.dagb40.0h1-20-outputSimple.txt','output/CyberShake_1000.dagb40.0h1-20-outputAware.txt')
#plot_experiment('output/CyberShake_1000.dagb80.0h1-20-outputSimple.txt','output/CyberShake_1000.dagb80.0h1-20-outputAware.txt')
#plot_experiment('output/CyberShake_1000.dagb120.0h1-20-outputSimple.txt','output/CyberShake_1000.dagb120.0h1-20-outputAware.txt')
#plot_experiment('output/CyberShake_1000.dagb160.0h1-20-outputSimple.txt','output/CyberShake_1000.dagb160.0h1-20-outputAware.txt')
#plot_experiment('output/CyberShake_1000.dagb200.0h1-20-outputSimple.txt','output/CyberShake_1000.dagb200.0h1-20-outputAware.txt')
#
#plot_experiment('output/Inspiral_1000.dagb400.0h1-40-outputSimple.txt','output/Inspiral_1000.dagb400.0h1-40-outputAware.txt')
#plot_experiment('output/Inspiral_1000.dagb800.0h1-40-outputSimple.txt','output/Inspiral_1000.dagb800.0h1-40-outputAware.txt')
#plot_experiment('output/Inspiral_1000.dagb1200.0h1-40-outputSimple.txt','output/Inspiral_1000.dagb1200.0h1-40-outputAware.txt')
#plot_experiment('output/Inspiral_1000.dagb1600.0h1-40-outputSimple.txt','output/Inspiral_1000.dagb1600.0h1-40-outputAware.txt')
#plot_experiment('output/Inspiral_1000.dagb2000.0h1-40-outputSimple.txt','output/Inspiral_1000.dagb2000.0h1-40-outputAware.txt')
#
#plot_experiment('output/Epigenomics_997.dagb8000.0h10-2000-outputSimple.txt','output/Epigenomics_997.dagb8000.0h10-2000-outputAware.txt')
#plot_experiment('output/Epigenomics_997.dagb16000.0h10-2000-outputSimple.txt','output/Epigenomics_997.dagb16000.0h10-2000-outputAware.txt')
#plot_experiment('output/Epigenomics_997.dagb24000.0h10-2000-outputSimple.txt','output/Epigenomics_997.dagb24000.0h10-2000-outputAware.txt')
#plot_experiment('output/Epigenomics_997.dagb30000.0h10-2000-outputSimple.txt','output/Epigenomics_997.dagb30000.0h10-2000-outputAware.txt')
#plot_experiment('output/Epigenomics_997.dagb40000.0h10-2000-outputSimple.txt','output/Epigenomics_997.dagb40000.0h10-2000-outputAware.txt')
#
#plot_experiment('output/Sipht_1000.dagb200.0h1-50-outputSimple.txt','output/Sipht_1000.dagb200.0h1-50-outputAware.txt')
#plot_experiment('output/Sipht_1000.dagb400.0h1-50-outputSimple.txt','output/Sipht_1000.dagb400.0h1-50-outputAware.txt')
#plot_experiment('output/Sipht_1000.dagb600.0h1-50-outputSimple.txt','output/Sipht_1000.dagb600.0h1-50-outputAware.txt')
#plot_experiment('output/Sipht_1000.dagb800.0h1-50-outputSimple.txt','output/Sipht_1000.dagb800.0h1-50-outputAware.txt')
#plot_experiment('output/Sipht_1000.dagb1000.0h1-50-outputSimple.txt','output/Sipht_1000.dagb1000.0h1-50-outputAware.txt')



#
#plot_experiment('output/Epigenomics_997.dag-outputSimple.txt','output/Epigenomics_997.dag-outputAware.txt')
#
#plot_experiment('output/Epigenomics_997.dagb35000.0h10-2000-outputSimple.txt','output/Epigenomics_997.dagb35000.0h10-2000-outputAware.txt')
#
#
#
#plot_experiment('output/Sipht_1000.dagb800.0h1-50-outputSimple.txt','output/Sipht_1000.dagb800.0h1-50-outputAware.txt')
#
#plot_experiment('output/Inspiral_1000.dagb1600.0h1-40-outputSimple.txt','output/Inspiral_1000.dagb1600.0h1-40-outputAware.txt')
#
#plot_experiment('output/CyberShake_1000.dagb100.0h1-20-outputSimple.txt','output/CyberShake_1000.dagb100.0h1-20-outputAware.txt')




# Generates inputs for experiments with diferent failure rates
# 
# Author: Maciej Malawski

prefix = "finish-variations-test"
applications = ["CYBERSHAKE", "GENOME", "LIGO", "MONTAGE", "SIPHT"]
input_dirs = {"CYBERSHAKE" => "CyberShake", "GENOME" => "Genome", "LIGO" => "LIGO", "MONTAGE" => "Montage", "SIPHT" => "SIPHT"}
algorithms = ["DPDS", "WADPDS", "SPSS"]
distributions = ["uniform_unsorted", "uniform_sorted", "pareto_unsorted", "pareto_sorted", "constant"]
variations = [0.00, 0.01, 0.02, 0.05, 0.10, 0.20, 0.50]
failure_rate = 0
ensemble_size = 50
id = 0

for seed in 0..9 do
  task_id = 0
  run_dir = "run-%s-%02d" % [prefix, seed]
  begin 
    Dir.mkdir run_dir
  rescue
  end
  file_name = run_dir + "/" + run_dir+".txt"
  f = File.open(file_name, "w")
  for variation in variations do
    for application in applications do
      for algorithm in algorithms do
        for distribution in distributions do
          id = id + 1
          task_id = task_id + 1
          id_string = "%08d" % id
          args = "-application #{application} -inputdir ../projects/pegasus/#{input_dirs[application]}/ -outputfile #{run_dir}/#{prefix}-output-#{id_string}.dat -distribution #{distribution} -ensembleSize #{ensemble_size} -algorithm #{algorithm} -seed #{seed} -failureRate #{failure_rate} -runtimeVariance #{variation}\n"
          f.write args
        end
      end  
    end
  end
  f.close
  #puts "./scripts/run_test_local.sh " + file_name + " &"
  puts "qsub -pe smp 2 -q short -t 1-#{task_id}  /afs/crc.nd.edu/user/m/mmalawsk/cloudworkflowsimulator/run_test_sge.sh " + file_name
end

puts `cd ~/cloudworkflowsimulator && ant`  
puts `cp ~/cloudworkflowsimulator/dist/cloudworkflowsimulator.jar /afs/crc.nd.edu/user/m/mmalawsk/cloudworkflowsimulator/dist/`  

#-application SIPHT -inputdir ../projects/pegasus/SIPHT/ -outputfile output.dat -distribution pareto_sorted -ensembleSize 50 -algorithm SPSS -seed 0 -failureRate 0.5

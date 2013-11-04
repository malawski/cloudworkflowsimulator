#!/usr/bin/env ruby
# Generates inputs for experiments with diferent failure rates
# 
# Author: Maciej Malawski
require 'pathname'

if ARGV.length == 0 then
  puts "Usage: ruby generate_delays.rb <DAG location prefix>"
  exit(-1)
end

dag_prefix = ARGV[0]
prefix = "finish-delays-test"
applications = ["CYBERSHAKE", "GENOME", "LIGO", "MONTAGE", "SIPHT"]
input_dirs = {"CYBERSHAKE" => "CyberShake", "GENOME" => "Genome", "LIGO" => "LIGO", "MONTAGE" => "Montage", "SIPHT" => "SIPHT"}
algorithms = ["DPDS", "WADPDS", "SPSS"]
distributions = ["uniform_unsorted", "uniform_sorted", "pareto_unsorted", "pareto_sorted", "constant"]
delays = [0, 30, 60, 90, 120, 300, 600, 900]
failure_rate = 0
ensemble_size = 50
variation = 0.0
id = 0

for seed in 0..9 do
  task_id = 0
  run_dir = "run-%s-%02d" % [prefix, seed]
  begin 
    Dir.mkdir run_dir
  rescue
  end
  file_name = run_dir + "/" + run_dir + ".txt"
  f = File.open(file_name, "w")
  for delay in delays do
    for application in applications do
      for algorithm in algorithms do
        for distribution in distributions do
          id = id + 1
          task_id = task_id + 1
          id_string = "%08d" % id
          args = "--application #{application} " +
            "--input-dir #{dag_prefix}/#{input_dirs[application]}/ " +
            "--output-file #{run_dir}/#{prefix}-output-#{id_string}.dat " +
            "--distribution #{distribution} --ensemble-size #{ensemble_size} " +
            "--algorithm #{algorithm} --seed #{seed} --failure-rate #{failure_rate} " +
            "--runtime-variance #{variation} --delay #{delay} " +
            "--storage-manager void" +
            "\n"
          f.write args
        end
      end  
    end
  end
  f.close
  mydir = Pathname.new(File.dirname(__FILE__)).realpath
  script_file = mydir + Pathname.new("../runners/run_simulation_set_locally.sh")
  input_file = mydir + Pathname.new(file_name)
  puts "echo \"#{script_file} #{input_file}\" | qsub -pe smp 2 -q l_test -t 1-#{task_id}"
end


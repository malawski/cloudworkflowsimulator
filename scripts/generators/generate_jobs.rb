#!/usr/bin/env ruby
# Generates inputs for experiments with diferent failure rates
require 'pathname'
require 'fileutils'
require 'optparse'

prefix = "generated"
applications = ["CYBERSHAKE", "GENOME"]
algorithms = ["DPDS", "WADPDS", "SPSS"]
input_dirs = {"CYBERSHAKE" => "CyberShake", "GENOME" => "Genome", "LIGO" => "LIGO", "MONTAGE" => "Montage", "SIPHT" => "SIPHT"}
distributions = ["pareto_unsorted", "pareto_sorted"]
delays = [0, 30]
storage_managers = ["void", "global"]
read_speeds = [10000000, 20000000]
write_speeds = [10000000, 20000000]
failure_rate = 0
ensemble_size = 20
variation = 0.0
seeds = 0..3
queue_name = "l_short"
dag_prefix = nil
out_prefix = nil

OptionParser.new do |opts|
  opts.banner = "Usage: #{__FILE__} --dag-prefix PREFIX --out-prefix PREFIX [additional options]"
  opts.on("--dag-prefix PREFIX", String, "Required dag path prefix. This is the folder with all your *.dag files. It should contain subfolders like \"CyberShake\" or \"LIGO\". Additionally, on grid environments this directory should be located on fast-access filesystem.") do |val|
    dag_prefix = val
  end
  opts.on("--out-prefix PREFIX", String, "Required output path prefix. This is the location that output files of this script will be located. On grid environments this should be on fast-access filesystem.") do |val|
    out_prefix = val
  end
  opts.on("--name-prefix PREFIX", String, "Optional input files' prefix, defaults to \"#{prefix}\". All input and output files of simulations will be prefixed with this string. Use meaningful names here to organize your files.") do |val|
    prefix = val
  end
  opts.on("--applications x,y", Array, "Optional applications list, defaults to #{applications}") do |val|
    applications = val
  end
  opts.on("--algorithms x,y", Array, "Optional list of algorithms, defaults to #{algorithms}") do |val|
    algorithms = val
  end
  opts.on("--distributions x,y", Array, "Optional list of distributions, defaults to #{distributions}") do |val|
    distributions = val
  end
  opts.on("--delays x,y", Array, "Optional list of delays, defaults to #{delays}") do |val|
    delays = val
  end
  opts.on("--seeds x,y", Array, "Optional list of seeds, defaults to #{seeds}") do |val|
    seeds = val
  end
  opts.on("--storage-managers x,y", Array, "Optional list of storage managers, defaults to #{storage_managers}") do |val|
    storage_managers = val
  end
  opts.on("--storage-manager-reads x,y", Array, "Optional list of storage manager read speeds, defaults to #{read_speeds}") do |val|
    read_speeds = val
  end
  opts.on("--storage-manager-writes x,y", Array, "Optional list of storage manager write speeds, defaults to #{write_speeds}") do |val|
    write_speeds = val
  end
  opts.on("--queue-name NAME", Array, "Optional queue name, defaults to \"#{queue_name}\"") do |val|
    seeds = val
  end
  opts.on("--failure_rate RATE", Float, "Optional failure rate, defaults to \"#{failure_rate}\"") do |val|
    failure_rate = val
  end
  opts.on("--ensemble_size SIZE", String, "Optional ensemble size, defaults to \"#{ensemble_size}\"") do |val|
    ensemble_size = val
  end
  opts.on("--variation VARIATION", Float, "Optional variation, defaults to \"#{variation}\"") do |val|
    variation = val
  end

  opts.on_tail("-h", "--help", "--usage", "Show this usage message and quit.") do |setting|
    puts opts.help
    exit
  end

  opts.parse!(ARGV)

  if dag_prefix == nil or out_prefix == nil then
    puts opts.help
    exit
  end
end


id = 0
for seed in seeds do
  task_id = 0
  run_dir_name = "run-%s-%02d" % [prefix, seed]
  run_dir = Pathname.new(out_prefix) + Pathname.new(run_dir_name)
  begin 
    FileUtils.mkdir_p run_dir
  rescue
  end
  file_name = run_dir + Pathname.new(run_dir_name + ".txt")
  f = File.open("#{file_name}", "w")
  for delay in delays do
    for application in applications do
      for algorithm in algorithms do
        for distribution in distributions do
          for storage_manager in storage_managers do
            for write_speed in write_speeds do
              for read_speed in read_speeds do
                id = id + 1
                task_id = task_id + 1
                id_string = "%08d" % id
                args = "--application #{application} " +
                  "--input-dir #{dag_prefix}/#{input_dirs[application]}/ " +
                  "--output-file #{run_dir}/#{prefix}-output-#{id_string}.dat " +
                  "--distribution #{distribution} " +
                  "--ensemble-size #{ensemble_size} " +
                  "--algorithm #{algorithm} " +
                  "--seed #{seed} " +
                  "--failure-rate #{failure_rate} " +
                  "--runtime-variance #{variation} " +
                  "--delay #{delay} " +
                  "--storage-manager #{storage_manager} " +
                  "--storage-manager-read #{read_speed} " +
                  "--storage-manager-write #{write_speed}" +
                  "\n"
                f.write args
              end
            end
          end
        end
      end  
    end
  end
  f.close
  mydir = Pathname.new(File.dirname(__FILE__)).realpath
  script_file = mydir + Pathname.new("../runners/run_simulation_set_locally.sh")
  input_file = mydir + Pathname.new(file_name)
  puts "echo \"#{script_file} #{input_file}\" | qsub -q #{queue_name}"
end


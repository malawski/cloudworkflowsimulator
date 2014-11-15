#!/usr/bin/env ruby
# Generates inputs and running commands for simulations. It generates all combinations of provided input parameters.
# Use --help command line parameter to see all available options.
require 'pathname'
require 'fileutils'
require 'optparse'

# Required parameters which should be provided by the user.
dag_dir = nil
out_dir = nil

# Parameters with default values. They can be overriden.
name_prefix = "generated"
applications = ["MONTAGE", "GENOME"]
algorithms = ["DPDS", "WADPDS", "SA-WADPDS", "SPSS", "SA-SPSS"]
input_dirs = {"CYBERSHAKE" => "CyberShake", "GENOME" => "Genome",
    "LIGO" => "LIGO", "MONTAGE" => "Montage", "SIPHT" => "SIPHT"}
distributions = ["pareto_unsorted"]
num_replicas = [100]
latencies = [1]
storage_managers = ["void", "global"]
read_speeds = [100000000]
write_speeds = [100000000]
failure_rate = 0
ensemble_size = 7
variation = 0.0
seeds = [Time.now().sec]
queue_name = "l_short"
qsub_params = ""
local = false
run = false
cws_logs = false 
clear = false


OptionParser.new do |opts|
  opts.banner = "Usage: #{__FILE__} --dag-dir DIR --out-dir DIR [additional options]"

  opts.on("--dag-dir DIR", String, "Required dag dir prefix. This is the folder with " +
  "all your *.dag files. It should contain subfolders like \"CyberShake\" or \"LIGO\". " +
  "Additionally, on grid environments this directory should be located on fast-access filesystem.") do |val|
    dag_dir = Pathname.new(val).realpath
  end
  opts.on("--out-dir DIR", String, "Required output path prefix. This is the location " +
  "that output files of this script will be located. On grid environments this should be " +
  "on fast-access filesystem. The directory will be created it it doesn't exist.") do |val|
    out_dir = val
  end
  opts.on("--name-prefix PREFIX", String, "Optional input files' prefix, defaults to " +
  "\"#{name_prefix}\". All input and output files of simulations will be prefixed with " +
  "this string. Use meaningful names here to organize your files.") do |val|
    name_prefix = val
  end
  opts.on("--applications x,y", Array, "Optional applications list, defaults to #{applications.join(',')}") do |val|
    applications = val
  end
  opts.on("--algorithms x,y", Array, "Optional list of algorithms, defaults to #{algorithms.join(',')}") do |val|
    algorithms = val
  end
  opts.on("--distributions x,y", Array, "Optional list of distributions, defaults to #{distributions.join(',')}") do |val|
    distributions = val
  end
  opts.on("--seeds x,y", Array, "Optional list of seeds, defaults to #{seeds.join(',')}") do |val|
    seeds = val
  end
  opts.on("--storage-managers x,y", Array, "Optional list of storage managers, " +
      "defaults to #{storage_managers.join(',')}") do |val|
    storage_managers = val
  end
  opts.on("--gs-read-speeds x,y", Array, "Optional list of gs-read-speeds " +
      ", defaults to #{read_speeds.join(',')}") do |val|
    read_speeds = val
  end
  opts.on("--gs-write-speeds x,y", Array, "Optional list of gs-write-speeds " +
      ", defaults to #{write_speeds.join(',')}") do |val|
    write_speeds = val
  end
  opts.on("--gs-replicas x,y", Array, "Optional list of the number of replicas for storage manager, " +
      "defaults to #{num_replicas.join(',')}") do |val|
    num_replicas = val
  end
  opts.on("--gs-latencies x,y", Array, "Optional list latencies for storage manager, " +
      "defaults to #{latencies.join(',')}") do |val|
    latencies = val
  end
  opts.on("--queue-name NAME", String, "Optional queue name, defaults to \"#{queue_name}\"") do |val|
    queue_name = val
  end
  opts.on("--qsub-params PARAMS", String, "Optional additional qsub parameters, defaults to empty string") do |val|
    qsub_params = val
  end
  opts.on("--failure-rate RATE", Float, "Optional failure rate, defaults to \"#{failure_rate}\"") do |val|
    failure_rate = val
  end
  opts.on("--ensemble-size SIZE", String, "Optional ensemble size, defaults to \"#{ensemble_size}\"") do |val|
    ensemble_size = val
  end
  opts.on("--variation VARIATION", Float, "Optional variation, defaults to \"#{variation}\"") do |val|
    variation = val
  end
  opts.on("-c", "--[no-]clear", "Whether to rm -r * in out directory, defaults to \"#{clear}\"") do |val|
    clear = val
  end
  opts.on("-v", "--[no-]visualise", "Whether to visualise results, defaults to \"#{cws_logs}\"") do |val|
    cws_logs = val
  end
  opts.on("-l", "--[no-]local", "Generate bash_commandands for running simulations locally, defaults to #{local}") do |val|
    local = val
  end
  opts.on("-r", "--[no-]run", "Run generated bash_commandands instead of printing them, defaults to #{run}") do |val|
    run = val
  end
  opts.on_tail("-h", "--help", "--usage", "Show this usage message and quit.") do |setting|
    puts opts.help
    exit
  end

  opts.parse!(ARGV)

  if dag_dir == nil or out_dir == nil then
    puts opts.help
    exit
  end
end


mydir = Pathname.new(File.dirname(__FILE__)).realpath
id = 0

if clear then
  cmd = "cd #{out_dir} && rm -r *"  
  `#{cmd}`
end

for seed in seeds do
  task_id = 0
  run_dir_name = "run-%s-%02d" % [name_prefix, seed]
  run_dir = Pathname.new(out_dir) + Pathname.new(run_dir_name)
  begin
    FileUtils.mkdir_p run_dir
  rescue
  end
  run_dir = run_dir.realpath
  
  file_name = run_dir + Pathname.new(run_dir_name + ".txt")
  file = File.open("#{file_name}", "w")

  for application in applications do
    for algorithm in algorithms do
      for distribution in distributions do
        void_manager_written = false
        for storage_manager in storage_managers do
          for write_speed in write_speeds do
            for read_speed in read_speeds do
              for num_replica in num_replicas do
                for latency in latencies do
                  id = id + 1
                  task_id = task_id + 1
                  id_string = "%08d" % id
                  args = "--application #{application} " +
                    "--input-dir #{dag_dir}/#{input_dirs[application]}/ " +
                  "--output-file #{run_dir}/#{name_prefix}-output-#{id_string}-#{application}-#{algorithm}-#{storage_manager}.dat " +
                  "--distribution #{distribution} " +
                    "--ensemble-size #{ensemble_size} " +
                    "--algorithm #{algorithm} " +
                    "--seed #{seed} " +
                    "--failure-rate #{failure_rate} " +
                    "--runtime-variance #{variation} " +
                    "--storage-manager #{storage_manager} " +
                    "--enable-logging #{cws_logs} " +
                    "--vm-directory #{mydir}/../../vms " +
                    "--global-storage-directory #{mydir}/../../gs "
                  if storage_manager != 'void' then
                    args = args + "--gs-read-speed #{read_speed} " +
                      "--gs-write-speed #{write_speed} " +
                      "--gs-latency #{latency} " +
                      "--gs-replicas #{num_replica}"
                    #elsif !void_manager_written then
                    #  void_manager_written = true
                    #else
                    #  break
                  end
                  args = args + "\n"
                  file.write args
                end
              end
            end
          end
        end
      end
    end
  end
  file.close
  absolute_running_script_path = mydir + Pathname.new("../runners/run_simulation_set_locally.sh")
  visualise_path = mydir + Pathname.new("../visualisation/visualize_all.sh")
  absolute_input_file_path = Pathname.new(".").realpath + Pathname.new(file_name)
  bash_command = nil
  absolute_out = Pathname.new(".").realpath + Pathname.new("#{run_dir}/#{name_prefix}-output")
  visualise = "#{visualise_path} #{absolute_out}"
  the_command = "#{absolute_running_script_path} #{absolute_input_file_path}"
  if cws_logs then
    the_command = "#{the_command} && #{visualise}"
  end
  if local then
    bash_command = the_command
  else 
    bash_command = "echo \"#{the_command}\" " +
      "| qsub -q #{queue_name} #{qsub_params}"
  end
  if run then
    puts "Running: #{bash_command}"
    `#{bash_command}`
  else 
    puts bash_command
  end
end


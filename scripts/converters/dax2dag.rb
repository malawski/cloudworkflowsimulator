require 'rubygems'
require 'nokogiri'


# Usage example:
#   ruby dax2dag.rb CyberShake_30.xml
# will produce CyberShake_30.dag


# Converts dax files into simplified dag format.
# Input is a DAX file from Workflow Generator
# https://confluence.pegasus.isi.edu/display/pegasus/WorkflowGenerator 
# In some DAX files, one file can appear multiple times with different sizes, 
# e.g. file FFI_0_1_subfx.sgt in CyberShake_30.xml.
# In such a case, we take the last file size.
# 
# Author: Maciej Malawski


# Processes <job> tag, modifies files and jobs structures
def process_job(node, files, jobs)
  job_name = node["name"]
  job_namespace = node["namespace"]
  job_version = node["version"]
  job_id = node["id"]
  job_runtime = node["runtime"]
  job_type = "#{job_namespace}::#{job_name}:#{job_version}"
  # puts  job_id + " " + job_type + " " + job_runtime 
  jobs[job_id] = {"type" => job_type, "runtime" => job_runtime, "input" => [], "output" => []}
  node.children.each { |node|
    if node.name=="uses"
      process_uses(node, job_id, files, jobs)
    end
  }
end

# Processes <uses> tag inside <job> of job_id, modifies files and jobs structures
def process_uses(node, job_id, files, jobs)
  file_name = node["file"]
  file_link = node["link"]
  file_size = node["size"]
  if files[file_name]
    #puts "Duplicate file: #{file_name}, size: #{file_size}, replacing."
  end
  files[file_name] = file_size
  #puts  "\t" + file_name + " " + file_link + " " + file_size
  jobs[job_id][file_link].push file_name
end

# Processes <child> tag, modifies edges structure
def process_child(node, edges)
  child_id = node["ref"]
  edges[child_id] = []
  node.children.each { |node|
    if node.name=="parent"
      parent_id = node["ref"]
      edges[child_id].push parent_id
      #puts "EDGE #{parent_id} #{child_id}"
    end
  }
end


####################################################################
# Main script
####################################################################

if ARGV.length!=1
  puts "Usage: ruby dax2dag.rb DAX_FILE"
  puts "   If DAX_FILE has .dax or .xml extension, it is replaced in output file by .dag."
  puts "   Otherwise .dag extension is appended."
  exit 0
end


input_file_name = ARGV[0]
f = File.open(input_file_name)

doc = Nokogiri::XML(f)

# maps file name to file size
files = Hash.new

# maps job id to a hash (type, runtime, input, output)
# input and output are arrays of file file names
jobs = Hash.new

# maps child id to array of parent ids
edges = Hash.new

# main loop
doc.children.each { |node|
  if node.name=='adag'
    node.children.each { |node|
      if node.name=="job"
        process_job(node, files, jobs)
      end
      if node.name=="child"
        process_child(node, edges)
      end
    }
  end
}
f.close

output_file_name = File.basename(input_file_name)
output_file_name = output_file_name.gsub(/\.(xml|dax)$/, "")
output_file_name << ".dag"


# print simplified DAG
File.open(output_file_name, 'w') { |f|

  files.each { |file_name, file_size|
    f.puts "FILE #{file_name} #{file_size}"
  }

  jobs.each { |id, job|
    f.puts "TASK #{id} #{job['type']} #{job['runtime']}"
  }

  jobs.each { |id, job|
    if !job['input'].empty?
      f.puts "INPUTS #{id} #{job['input'].join(' ')}"
    end
    if !job['output'].empty?
      f.puts "OUTPUTS #{id} #{job['output'].join(' ')}"
    end
  }

  edges.each { |child_id, parents|
    parents.each { |parent_id|
      f.puts "EDGE #{parent_id} #{child_id}"
    }
  }
}
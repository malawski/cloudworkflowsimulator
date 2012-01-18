require "#{File.dirname(__FILE__)}/plots.rb"

ARGV.each do|a|
  plot_schedule(a)
end
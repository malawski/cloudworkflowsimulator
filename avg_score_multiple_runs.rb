#!/usr/bin/env ruby

require 'roo'
require 'csv'
require 'optparse'

REQUIRED_OPTIONS=['date', 'logfile', 'runs', 'budgets', 'deadline_col', 'budgets_col', 'exp_score_col', 'output']
options = {}
OptionParser.new do |opts|
  opts.banner = 'Usage: visualize_exp_score.rb [options]'

  opts.on('-i', '--date SIM_DATE', 'Mandatory simulation date') do |v|
    options['date'] = v
  end
  opts.on('-l', '--logfile LOGFILE', 'Mandatory logfile name') do |v|
    options['logfile'] = v
  end
  opts.on('-r', '--runs RUNS', 'Mandatory runs amount') do |v|
    options['runs'] = v.to_i
  end
  opts.on('-n', '--nbudgets N_BUDGETS', 'Mandatory budgets amount') do |v|
    options['budgets'] = v.to_i
  end
  opts.on('-d', '--deadline_col DEADLINE_COL', 'Mandatory deadlines column number') do |v|
    options['deadline_col'] = v.to_i
  end
  opts.on('-b', '--budgets_col BUDGETS_COL', 'Mandatory budgets column number') do |v|
    options['budgets_col'] = v.to_i
  end
  opts.on('-s', '--exp_score_col EXP_SCORE_COL', 'Mandatory score column number') do |v|
    options['exp_score_col'] = v.to_i
  end
  opts.on('-o', '--output OUTPUT', 'Mandatory output file name') do |v|
    options['output'] = v
  end
end.parse!

raise OptionParser::MissingArgument if (REQUIRED_OPTIONS - options.keys).length != 0

csvs = []
for i in 1..options['runs']
  csvs.push("#{options['date']}/#{i}/csv/#{options['logfile']}_#{i}.csv")
end

each_csv_scores = []
csvs.each do |csv|
  bla = Roo::CSV.new(csv)
  ble = []
  bla.column(options['exp_score_col']).each do |score|
    next if score=="exponential"
    ble.push(score.to_f)
  end
  each_csv_scores.push(ble)
end

each_csv_scores_summed = each_csv_scores.transpose.map { |x| x.reduce(:+) }

each_csv_scores_summed_mean = []
each_csv_scores_summed.each do |a|
  if a > 0
    each_csv_scores_summed_mean.push(a/options['budgets'])
  else
    each_csv_scores_summed_mean.push(a)
  end
end

csv = Roo::CSV.new("#{options['date']}/1/csv/#{options['logfile']}_1.csv")
deadlines = csv.column(options['deadline_col'])
budgets = csv.column(options['budgets_col'])

CSV.open(options['output'], 'wb') do |file|
  file << ["budget", "deadline", "expontential"]
  each_csv_scores_summed_mean.each_index do |i|
    file << [budgets[i+1], deadlines[i+1], each_csv_scores_summed_mean[i]]
  end
end

require 'roo'
require 'csv'

SIM_DATE=ARGV[0]
LOGFILE=ARGV[1]
RUNS=ARGV[2].to_i
N_BUDGETS=ARGV[3].to_i
DEADLINE_COL = ARGV[4].to_i
BUDGETS_COL = ARGV[5].to_i
EXP_SCORE_COL = ARGV[6].to_i
OUTPUT=ARGV[7]

csvs = []
for i in 1..RUNS
  csvs.push("#{SIM_DATE}/#{i}/csv/#{LOGFILE}_#{i}.csv")
end

each_csv_scores = []
csvs.each do |csv|
  bla = Roo::CSV.new(csv)
  ble = []
  bla.column(EXP_SCORE_COL).each do |score|
    next if score=="exponential"
    ble.push(score.to_f)
  end
  each_csv_scores.push(ble)
end

each_csv_scores_summed = each_csv_scores.transpose.map { |x| x.reduce(:+) }

each_csv_scores_summed_mean = []
each_csv_scores_summed.each do |a|
  if a > 0
    each_csv_scores_summed_mean.push(a/N_BUDGETS)
  else
    each_csv_scores_summed_mean.push(a)
  end
end

csv = Roo::CSV.new("#{SIM_DATE}/1/csv/#{LOGFILE}_1.csv")
deadlines = csv.column(DEADLINE_COL)
budgets = csv.column(BUDGETS_COL)

CSV.open(OUTPUT, 'wb') do |file|
  file << ["budget", "deadline", "expontential"]
  each_csv_scores_summed_mean.each_index do |i|
    file << [budgets[i+1], deadlines[i+1], each_csv_scores_summed_mean[i]]
  end
end

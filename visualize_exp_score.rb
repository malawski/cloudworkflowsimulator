#!/usr/bin/env ruby

require 'gnuplot'
require 'roo'
require 'optparse'
require 'json'


def extract_available_budgets(budgets_column)
  budgets = []

  budgets_column.drop(1).each do |budget|
    unless budgets.include? budget
      budgets.push(budget)
    end
  end

  budgets
end

def extract_available_deadlines(csv, options)
  csv.column(options['deadline_col']).drop(1).uniq
end

def extract_max_deadline_value(csv, options)
  deadlines = extract_available_deadlines(csv, options)
  max_deadline = 0

  deadlines.each do |deadline|
    current = deadline.to_f
    if current > max_deadline
      max_deadline = current
    end
  end

  max_deadline
end

def extract_min_deadline_value(csv, options)
  csv.cell(options['min_deadline_row'], options['deadline_col'])
end

def normalize(min, max, current)
  (current.to_f - min.to_f) / (max.to_f - min.to_f)
end

def normalize_deadlines(csv, deadlines, options)
  min_deadline = extract_min_deadline_value(csv, options)
  max_deadline = extract_max_deadline_value(csv, options)
  normalized = []

  deadlines.each do |deadline|
    normalized.push(normalize(min_deadline, max_deadline, deadline))
  end

  normalized
end

#todo draw and draw_multiple should be one function, refactor
def draw(options)
  csv = Roo::CSV.new(options['csv'])
  budgets = extract_available_budgets(csv.column(options['budgets_col']))
  normalized_deadlines = normalize_deadlines(csv, extract_available_deadlines(csv, options), options)

  budgets.each_index do |i|
    budget = budgets[i]
    scores_for_budget = []
    first_exp_score_row = i*options['budgets']+1
    last_exp_score_row = first_exp_score_row + options['budgets'] - 1

    (first_exp_score_row...last_exp_score_row).each do |exp_row|
      scores_for_budget.push(csv.cell(exp_row+1, options['exp_score_col']).to_f)
    end

    Gnuplot.open do |gp|
      Gnuplot::Plot.new(gp) do |plot|
        plot.terminal 'png'
        plot.output File.expand_path("../Exponential_score_#{budget}.png", __FILE__)

        plot.title "Score for budget = #{budget}"
        plot.xlabel 'Normalized deadline'
        plot.ylabel 'Exponential score'
        plot.xrange '[0:1]'

        plot.data << Gnuplot::DataSet.new([normalized_deadlines, scores_for_budget]) do |ds|
          ds.with = 'linespoints'
          ds.linewidth = 4
          ds.notitle
        end
      end
    end
  end
end

def draw_multiple(options)
  csv = Roo::CSV.new(options[0]['csv'])
  budgets = extract_available_budgets(csv.column(options[0]['budgets_col']))
  normalized_deadlines = normalize_deadlines(csv, extract_available_deadlines(csv, options[0]), options[0])

  budgets.each_index do |i|
    budget = budgets[i]
    scores_for_algo = {}

    options.each do |opt|
      current_csv = Roo::CSV.new(opt['csv'])
      scores_for_budget = []
      first_exp_score_row = i*opt['budgets']+1
      last_exp_score_row = first_exp_score_row + opt['budgets'] - 1

      (first_exp_score_row...last_exp_score_row).each do |exp_row|
        scores_for_budget.push(current_csv.cell(exp_row+1, opt['exp_score_col']).to_f)
      end

      scores_for_algo[opt['algo']] = scores_for_budget
    end

    current_plot_data = scores_for_algo.keys.map do |algo|
      Gnuplot::DataSet.new([normalized_deadlines, scores_for_algo[algo]]) do |ds|
        ds.with = 'linespoints'
        ds.title = algo
      end
    end

    Gnuplot.open do |gp|
      Gnuplot::Plot.new(gp) do |plot|
        plot.terminal 'png'
        plot.output File.expand_path("../Exponential_score_#{budget}.png", __FILE__)

        plot.title "Score for budget = #{budget}"
        plot.xlabel 'Normalized deadline'
        plot.ylabel 'Exponential score'
        plot.xrange '[0:1]'
        plot.key 'right bottom'

        plot.data = current_plot_data
      end
    end
  end
end

multiple_mode = false

REQUIRED_OPTIONS=['csv', 'budgets', 'min_deadline_row', 'deadline_col', 'budgets_col', 'exp_score_col']
options = {}
OptionParser.new do |opts|
  opts.banner = 'Usage: visualize_exp_score.rb [options]'

  opts.on('-c', '--csv SIM_CSV', 'Mandatory csv filepath') do |v|
    options['csv'] = v
  end
  opts.on('-n', '--budgets N_BUDGETS', 'Mandatory budgets amount') do |v|
    options['budgets'] = v.to_i
  end
  opts.on('-r', '--min_deadline_row MIN_DEADLINE_ROW', 'Mandatory minimum deadline row number') do |v|
    options['min_deadline_row'] = v.to_i
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
  opts.on('-m', '--multiple PATH_TO_CONFIG_JSON', 'Optional param, if set then you must provide
                          path to json config file for drawing scores for multiple algorithms on one graph') do |v|
    options['json_config'] = v
    multiple_mode = true
  end
end.parse!

if multiple_mode
  json_file = File.open(options['json_config'])
  config = JSON.parse(json_file.read)
  json_file.close
  draw_multiple(config)
else
  raise OptionParser::MissingArgument if (REQUIRED_OPTIONS - options.keys).length != 0
  draw(options)
end

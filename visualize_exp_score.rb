require 'gnuplot'
require 'roo'

$combinations_amount

SIM_CSV = ARGV[0]
SIM_OUT_CSV = Roo::CSV.new(SIM_CSV)

MIN_DEADLINE_ROW = 2
DEADLINE_COL = 7
BUDGETS_COLUMN_NO = 6
EXP_SCORE_COL_NO = 10
EXPONENTIAL_SCORE_COLUMN = SIM_OUT_CSV.column(EXP_SCORE_COL_NO)

def extract_max_deadline_value
  min_deadline = extract_min_deadline_value
  all_deadlines = SIM_OUT_CSV.column(DEADLINE_COL)
  tmp_row_index = 1

  all_deadlines.drop(2).each do |deadline_val|
    if deadline_val == min_deadline
      $combinations_amount = tmp_row_index
      return SIM_OUT_CSV.cell(tmp_row_index+1, DEADLINE_COL)
    else
      tmp_row_index += 1
    end
  end
end

def extract_min_deadline_value
  SIM_OUT_CSV.cell(MIN_DEADLINE_ROW, DEADLINE_COL)
end

def normalize(min, max, current)
  (current.to_f - min.to_f) / (max.to_f - min.to_f)
end

def extract_available_budgets(budgets_column)
  budgets = []

  budgets_column.drop(1).each do |budget|
    unless budgets.include? budget
      budgets.push(budget)
    end
  end

  budgets
end

def extract_available_deadlines
  SIM_OUT_CSV.column(DEADLINE_COL).drop(1).uniq
end

MIN_DEADLINE = extract_min_deadline_value
MAX_DEADLINE = extract_max_deadline_value

def normalize_deadlines(deadlines)
  normalized = []

  deadlines.each do |deadline|
    normalized.push(normalize(MIN_DEADLINE, MAX_DEADLINE, deadline))
  end

  normalized
end

budgets = extract_available_budgets(SIM_OUT_CSV.column(BUDGETS_COLUMN_NO))
normalized_deadlines = normalize_deadlines(extract_available_deadlines)

budgets.each_index do |i|
  budget = budgets[i]
  scores_for_budget = []
  first_exp_score_row = i*$combinations_amount+1
  last_exp_score_row = first_exp_score_row + $combinations_amount - 1

  (first_exp_score_row...last_exp_score_row).each do |exp_row|
    scores_for_budget.push(SIM_OUT_CSV.cell(exp_row+1, EXP_SCORE_COL_NO).to_f)
  end

  Gnuplot.open do |gp|
    Gnuplot::Plot.new( gp ) do |plot|
      plot.terminal 'png'
      plot.output File.expand_path('../Exponential_score_'+budget+'.png', __FILE__)

      plot.title budget
      plot.xlabel 'Normalized deadline'
      plot.ylabel 'Exponential score'
      plot.xrange '[0:1]'

      plot.data << Gnuplot::DataSet.new( [normalized_deadlines, scores_for_budget] ) do |ds|
        ds.with = 'linespoints'
        ds.linewidth = 4
        ds.notitle
      end
    end
  end
end

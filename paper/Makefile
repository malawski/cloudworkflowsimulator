LATEX = pdflatex -output-directory build
BIBTEX = bibtex
BUILD = build

T = paper

DEPS := $(shell ls *.tex *.bib *.cls)

all: $(T).pdf

$(T).pdf : $(DEPS)
	mkdir -p $(BUILD)
	$(LATEX) $(T)
	$(BIBTEX) $(BUILD)/$(T)
	$(LATEX) $(T)
	$(LATEX) $(T)
	cp $(BUILD)/$(T).pdf $(PWD)

clean: 
	$(RM) -r $(BUILD) $(T).pdf

all: main.tex main.bib
	pdflatex main.tex && \
	bibtex main && \
	pdflatex main.tex && \
	pdflatex main.tex

clean:
	rm -f *.log *.blg *.bbl *.aux *.out *.synctex.gz
	
realclean: clean
	rm -f main.pdf 

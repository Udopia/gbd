all: slides.pdf
	
slides.pdf: slides.tex
	pdflatex slides.tex && \
	pdflatex slides.tex
	
clean:
	rm -f slides.log slides.blg slides.bbl slides.aux slides.out slides.synctex.gz slides.snm slides.toc slides.nav
	
realclean: clean
	rm -f slides.pdf cnf.pdf gates.pdf

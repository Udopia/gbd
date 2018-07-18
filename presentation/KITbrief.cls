%%
%% This is file `KITbrief.cls',
%% generated with the docstrip utility.
%%
%% The original source files were:
%%
%% KITbrief.dtx  (with options: `brief,grid')
%%
%% This is a generated file.
%%
%%
%% Copyright (C) 2010, 2011 by Klaus Braune <klaus.braune@kit.edu>,
%% Steinbuch Centre for Computing (SCC) at Karlsruhe Institute of Technology (KIT)
%%
%%
%% This file may be distributed and/or modified under the conditions of
%% the LaTeX Project Public License, either version 1.2 of this license
%% or (at your option) any later version.  The latest version of this
%% license is in:
%%
%%    http://www.latex-project.org/lppl.txt
%%
%% and version 1.2 or later is part of all distributions of LaTeX version
%% 1999/12/01 or later.
%%
\NeedsTeXFormat{LaTeX2e}[1999/12/01]
\RequirePackage{rcs}
\RCS $Date: 2012/01/13 11:11:17 $
\RCS $Revision: 1.6 $
\ProvidesClass{KITbrief}
    [\RCSRawDate\space v\RCSRevision\space Document class KITbrief]
\newif\ifKIT@hyperref\KIT@hyperreffalse
\DeclareOption{hyperref}{\KIT@hyperreftrue}
\newif\ifKIT@RFrame\KIT@RFrametrue
\newcommand{\KITpaper}{\KIT@RFramefalse\KIT@Internfalse}
\let\Schmuckbogen=\KITpaper
\newcommand{\Mediaframe}{\KIT@RFrametrue\KIT@Internfalse}
\let\Medienrahmen=\Mediaframe
\DeclareOption{KITpaper}{\KITpaper}
\DeclareOption{schmuckbogen}{\KITpaper}
\newif\ifKIT@Intern\KIT@Internfalse
\newcommand{\Internal}{\KIT@Interntrue\KIT@RFrametrue%
  \colorlet{KITfoot}{white}}
\let\Intern=\Internal
\DeclareOption{internal}{\AtEndOfClass{\Internal}}
\DeclareOption{intern}{\AtEndOfClass{\Internal}}
\DeclareOption{CS}{\AtEndOfClass{\CS}}
\DeclareOption{CN}{\AtEndOfClass{\CN}}
\DeclareOption{Uni}{\AtEndOfClass{\Uni}}
\DeclareOption{Stud}{\AtEndOfClass{\Stud}}
\DeclareOption{LRS}{\AtEndOfClass{\LRS}}
\DeclareOption{KIT}{\AtEndOfClass{\KIT}}
\DeclareOption{noaccount}{\AtEndOfClass{\NoAccount}}
\DeclareOption{datenum}{\AtEndOfClass{\DateNumbers}}
\DeclareOption{datetxt}{\AtEndOfClass{\DateText}}
\newif\ifKIT@pagenum\KIT@pagenumtrue
\let\Nopagenumbers=\KIT@pagenumfalse
\let\Unnumbered=\KIT@pagenumfalse
\let\Unnummeriert=\KIT@pagenumfalse
\DeclareOption{nopagenumbers}{\KIT@pagenumtrue}
\DeclareOption{unnumbered}{\KIT@pagenumtrue}
\DeclareOption{unnummeriert}{\KIT@pagenumtrue}
\def\KIT@clrmod{cmyk}
\DeclareOption{cmyk}{\def\KIT@clrmod{cmyk}}
\DeclareOption{rgb}{\def\KIT@clrmod{rgb}}
\def\KIT@cmod{\BW}
\DeclareOption{bw}{\def\KIT@cmod{\BW}}
\DeclareOption{sw}{\def\KIT@cmod{\BW}}
\DeclareOption{color}{\def\KIT@cmod{\Color}}
\DeclareOption{farbig}{\def\KIT@cmod{\Color}}
\newif\ifKIT@De\KIT@Detrue
\DeclareOption{deutsch}{\AtEndOfClass{\German}%
  \PassOptionsToPackage{english,ngerman}{babel}}
\DeclareOption{german}{\AtEndOfClass{\German}%
  \PassOptionsToPackage{english,ngerman}{babel}}
\DeclareOption{ngerman}{\AtEndOfClass{\German}%
  \PassOptionsToPackage{english,ngerman}{babel}}
\DeclareOption{germanb}{\AtEndOfClass{\German}%
  \PassOptionsToPackage{english,ngerman}{babel}}
\DeclareOption{englisch}{\AtEndOfClass{\English}%
  \PassOptionsToPackage{ngerman,english}{babel}}
\DeclareOption{english}{\AtEndOfClass{\English}%
  \PassOptionsToPackage{ngerman,english}{babel}}
\DeclareOption{UKenglish}{\AtEndOfClass{\English}%
  \PassOptionsToPackage{ngerman,english}{babel}}
\DeclareOption{USenglish}{\AtEndOfClass{\English}%
  \PassOptionsToPackage{ngerman,english}{babel}}
\DeclareOption{twoside}{\PassOptionsToClass{twoside}{dinbrief}}
\newif\ifKIT@grid\KIT@gridfalse
\let\Kgrid=\KIT@gridtrue
\let\KIT@ClOpts=\relax
\DeclareOption*{\IfFileExists{\CurrentOption.tex}%
  {\AtEndOfClass{\input{\CurrentOption.tex}}}%
  {\PassOptionsToClass{\CurrentOption}{dinbrief}}}
\ProcessOptions
\ifx\KIT@ClOpts\relax
  \PassOptionsToClass{a4paper}{dinbrief}
\else
  \PassOptionsToClass{\KIT@ClOpts}{dinbrief}
\fi
\PassOptionsToPackage{\KIT@clrmod}{KITcolors}
\LoadClass[a4paper]{dinbrief}
\RequirePackage{mathpazo}
\RequirePackage{uarial}
\RequirePackage[TS1,T1]{fontenc}
\ifKIT@hyperref\RequirePackage{hyperref}\fi
\RequirePackage{bophook}
\def\KIT@State{}
\RequirePackage{KITcolors}
\colorlet{KITrframe}{KITblack}
\colorlet{KIT@foot}{KITblack50}
\colorlet{KITfoot}{KIT@foot}
\RequirePackage{KITdefs}
\RequirePackage{graphicx}
\RequirePackage{tikz}
\RequirePackage{calc,refcount}
\KIT@Framewd=5mm
\KIT@RFramewd=.5pt
\KIT@Linewd=.5pt
\KIT@MFllx=12.5mm
\KIT@MFurx=197.5mm
\KIT@MFury=287mm
\newcommand{\KIT@MFodd}{5mm}
\newcommand{\KIT@MFHdh}{10mm}
\newcommand{\KIT@MFbadd}{15mm}
\newcounter{KIT@ltr}
\setlength{\oddsidemargin}{20mm-1in}
\setlength{\evensidemargin}{20mm-1in}
\renewcommand{\familydefault}{\sfdefault}
\textwidth=170mm
\textheight=256mm
\addtolength{\topmargin}{-4.9mm}
\let\KIT@pgsz=\footnotesize
\enabledraftstandard
\nowindowrules
\nowindowtics
\newsavebox{\KIT@box}
\newsavebox{\KIT@boxii}
\let\KIT@letter=\letter
\long\def\letter#1{%
  \savebox{\KIT@box}[55mm][l]{%
    \parbox[t]{60mm}{\raggedright%
      \fontsize{8pt}{10.75pt}\selectfont%
      \rule{0pt}{9.5mm}%
      \if\KIT@InstI\relax\relax
        \else{\fontseries{b}\selectfont\KIT@InstI}\\\fi
      \if\KIT@InstII\relax\relax
        \else{\fontseries{b}\selectfont\KIT@InstII}\\\fi
      \vspace{1mm}
      \settowidth{\KIT@Wd}{\KIT@PhoTxtD:~}
        \fontsize{8pt}{10pt}\selectfont%
        \if\KIT@IHd\relax\relax
          \else
            \settowidth{\KIT@tmplen}{\KIT@IHdTxt:~}
            \parbox[t]{60mm}{\raggedright
              \ifdim\KIT@tmplen>\KIT@Wd\KIT@IHdTxt:
                \else\makebox[\KIT@Wd][l]{\KIT@IHdTxt:}\fi
              \KIT@IHd}\\[2.5mm]\fi%
      \fontsize{8pt}{10pt}\selectfont%
      \KIT@Str\if\KIT@Geb\relax\relax\else, \KIT@GebTxt~\KIT@Geb\fi\\
      \KIT@Ort\ifKIT@internat, \KIT@Cntry\fi\\[2.5mm]
      \settowidth{\KIT@tmplen}{\KIT@PhoTxt:~}
      \ifdim\KIT@tmplen>\KIT@Wd\KIT@PhoTxt:
        \else\makebox[\KIT@Wd][l]{\KIT@PhoTxt:}\fi
      \if\KIT@Pho\relax\KIT@PhoI-\KIT@PhoD\else\KIT@Pho\fi\\
      \settowidth{\KIT@tmplen}{\KIT@FaxTxt:~}
      \ifdim\KIT@tmplen>\KIT@Wd\KIT@FaxTxt:
        \else\makebox[\KIT@Wd][l]{\KIT@FaxTxt:}\fi
      \if\KIT@Fax\relax\KIT@FaxI-\KIT@FaxD\else\KIT@Fax\fi\\
      \settowidth{\KIT@tmplen}{\KIT@EmlTxt:~}
      \ifdim\KIT@tmplen>\KIT@Wd\KIT@EmlTxt:
        \else\makebox[\KIT@Wd][l]{\KIT@EmlTxt:}\fi
      \KIT@EmlName\KIT@EmlDomain\\
      \settowidth{\KIT@tmplen}{\KIT@WebTxt:~}
      \ifdim\KIT@tmplen>\KIT@Wd\KIT@WebTxt:
        \else\makebox[\KIT@Wd][l]{\KIT@WebTxt:}\fi
      \parbox[t]{36mm}{\raggedright \KIT@PWWW}\\%[2.5mm]
      \vspace{2.5mm}
      \if\KIT@Oic\relax\relax\else
        \settowidth{\KIT@WdF}{\KIT@RefTxtD:~}
        \settowidth{\KIT@tmplen}{\KIT@OicTxt:~}
        \ifdim\KIT@tmplen>\KIT@WdF\KIT@OicTxt:\quad
          \else\makebox[\KIT@WdF][l]{\KIT@OicTxt:}\fi
        \KIT@Oic\\%[2.5mm]%
      \fi
        \settowidth{\KIT@tmplen}{\KIT@RefTxt:~}
        \ifdim\KIT@tmplen>\KIT@WdF\KIT@RefTxt:
          \else\makebox[\KIT@WdF][l]{\KIT@RefTxt:}\fi
        \KIT@Ref\\
        \settowidth{\KIT@tmplen}{\KIT@DatTxt:~}
        \ifdim\KIT@tmplen>\KIT@Wd\KIT@DatTxt:
          \else\makebox[\KIT@Wd][l]{\KIT@DatTxt:}\fi
        \KIT@date
      }%
    \hss}%
  \settodepth{\KIT@Wd}{\usebox{\KIT@box}}%
  \addtolength{\KIT@Wd}{-\KIT@Spc}%
  \ifdim\KIT@Wd<0pt%
    \global\KIT@vsk=0pt%
   \else%
    \global\KIT@vsk=\KIT@Wd%
   \fi%
  \stepcounter{KIT@ltr}
  \xdef\KIT@pages{KIT@lpage\theKIT@ltr}
  \def\Pages{\pageref{\KIT@pages}}
  \let\Seiten=\Pages
    \KIT@letter{#1}
    \setlength{\KIT@AddrHt}{50mm}
}
\let\KIT@endletter=\endletter
\def\endletter{\label{\KIT@pages}\KIT@endletter}
\let\KIT@Conc=\subject
\renewcommand{\subject}[1]{\KIT@Conc{\bfseries\KIT@vsp\\#1}}
\let\concern=\subject
\let\Betreff=\subject
\newcommand{\Reference}[1]{\gdef\KIT@Ref{#1}}
\let\Zeichen=\Reference
\let\KIT@Ref=\relax
\let\KIT@RefTxt=\relax
\gdef\yourmail#1{\ClassWarning{KITbrief}{The command \string\yourmail{}
    is not supported by this class - use \string\Reference{} instead}}
\gdef\IZ#1{\ClassWarning{KITbrief}{The command \string\IZ{}
    is not supported by this class - use \string\Reference{} instead}}
\let\writer=\Official
\gdef\phone#1#2{\Phone{#1-#2}}
\let\@ntoday=\relax
\let\backaddress=\Absender
\def\ps@KIT{%
  \headheight=\lts@headheight
  \headsep=\lts@headsep
  \advance\headsep by6mm
  \footskip 10.0truemm
  \def\@oddhead{\mbox{}\hfill{\KIT@pgsz\thepage}}
  \def\@oddfoot{%
    \color{KITfoot}%
    \fontsize{6pt}{8pt}\selectfont\KIT@Text\hfill
    \fontsize{15pt}{18pt}\bfseries\KIT@Web}
  \def\@evenhead{{\KIT@pgsz\thepage}\hfill\mbox{}}
  \let\@evenfoot=\@oddfoot
  }
\newlength{\KIT@Wd}
\newlength{\KIT@WdF}
\newlength{\KIT@Spc}
\setlength{\KIT@Spc}{23mm}
\newlength{\KIT@vsk}
\newlength{\KIT@AddrHt}
\let\KIT@PrsTxt=\relax
\gdef\KIT@Prs{Prof.~Dr.~Horst Hippler, Prof.~Dr.~Eberhard Umbach}
\let\KIT@VPrsTxt=\relax
\gdef\KIT@VPra{Dr.~Elke Luise Barnstedt, Dr.~Ulrich Breuer,}
\gdef\KIT@VPrb{Dr.-Ing.~Peter Fritz, Prof.~Dr.-Ing.~Detlef L\"ohe}
\let\KITWWW=\KITURL
\let\KIT@Abs=\relax
\newcommand{\Returnaddress}[1]{\gdef\KIT@Abs{#1}}
\let\Absender=\Returnaddress
\gdef\KIT@CS@Abs{KIT--Campus S\"ud
  \ifx\KIT@FInst\relax \relax \else \vrule{} \KIT@FInst{} \fi
  \vrule{} Postfach 6980 \vrule{} 76049 Karlsruhe}
\gdef\KIT@CS@Str{Kaiserstr.~12}
\gdef\KIT@CS@Ort{76131 Karlsruhe}
\gdef\KIT@CS@PhoI{+49 721 608}
\let\KIT@CS@FaxI=\KIT@CS@PhoI
\gdef\CS{%
  \let\KIT@Abs=\KIT@CS@Abs
  \let\KIT@Str=\KIT@CS@Str
  \let\KIT@Ort=\KIT@CS@Ort
  \let\KIT@PhoI=\KIT@CS@PhoI
  \let\KIT@FaxI=\KIT@CS@FaxI
  }
\gdef\KIT@CN@Abs{KIT--Campus Nord
  \ifx\KIT@FInst\relax \relax \else \vrule{} \KIT@FInst{} \fi
  \vrule{} Postfach 3640 \vrule{} 76021 Karlsruhe}
\gdef\KIT@CN@Str{Herman-von-Helmholtz-Platz 1}
\gdef\KIT@CN@Ort{76344 Eggenstein-Leopoldshafen}
\gdef\KIT@CN@PhoI{+49 7247 82}
\let\KIT@CN@FaxI=\KIT@CN@PhoI
\gdef\CN{%
  \let\KIT@Abs=\KIT@CN@Abs
  \let\KIT@Str=\KIT@CN@Str
  \let\KIT@Ort=\KIT@CN@Ort
  \let\KIT@PhoI=\KIT@CN@PhoI
  \let\KIT@FaxI=\KIT@CN@FaxI
  }
\gdef\KIT@UStD{USt-IdNr.~DE266749428}
\let\KIT@UStE=\KIT@UStD
\gdef\KIT@U@SectD{Universit\"atsbereich}
\let\KIT@U@SectE=\KIT@U@SectD
\gdef\KIT@U@BankD{Bundesbank Karlsruhe}
\let\KIT@U@BankE=\KIT@U@BankD
\gdef\KIT@U@Kto{BLZ~660~000~00 \vrule{} Kto.~66~001~508}
\gdef\KIT@U@BIC{BIC/SWIFT:~MARK~DE~F1660}
\gdef\KIT@U@IBAN{IBAN:~DE57~6600~0000~0066~0015~08}
\gdef\Uni{%
  \KIT@Internfalse
  \colorlet{KITfoot}{KIT@foot}
  \ifKIT@De
    \let\KIT@Sect=\KIT@U@SectD
    \let\KIT@Bank=\KIT@U@BankD
  \else
    \let\KIT@Sect=\KIT@U@SectE
    \let\KIT@Bank=\KIT@U@BankE
  \fi
  \let\KIT@SectD=\KIT@U@SectD
  \let\KIT@BankD=\KIT@U@BankD
  \let\KIT@SectE=\KIT@U@SectE
  \let\KIT@BankE=\KIT@U@BankE
  \let\KIT@Kto=\KIT@U@Kto
  \let\KIT@BIC=\KIT@U@BIC
  \let\KIT@IBAN=\KIT@U@IBAN
}
\gdef\KIT@S@SectD{Universit\"atsbereich (Studiengeb\"uhren)}
\let\KIT@S@SectE=\KIT@S@SectD
\let\KIT@S@BankD=\KIT@U@BankD
\let\KIT@S@BankE=\KIT@U@BankD
\gdef\KIT@S@Kto{BLZ~660~000~00 \vrule{} Kto.~6600~1535}
\let\KIT@S@BIC=\KIT@U@BIC
\gdef\KIT@S@IBAN{IBAN:~DE07~6600~0000~0066~0015~35}
\gdef\Stud{%
  \KIT@Internfalse
  \colorlet{KITfoot}{KIT@foot}
  \ifKIT@De
    \let\KIT@Sect=\KIT@S@SectD
    \let\KIT@Bank=\KIT@S@BankD
  \else
    \let\KIT@Sect=\KIT@S@SectE
    \let\KIT@Bank=\KIT@S@BankE
  \fi
  \let\KIT@SectD=\KIT@S@SectD
  \let\KIT@BankD=\KIT@S@BankD
  \let\KIT@SectE=\KIT@S@SectE
  \let\KIT@BankE=\KIT@S@BankE
  \let\KIT@Kto=\KIT@S@Kto
  \let\KIT@BIC=\KIT@S@BIC
  \let\KIT@IBAN=\KIT@S@IBAN
}
\gdef\KIT@G@SectD{Gro\ss{}forschungsbereich}
\gdef\KIT@G@SectE{Large-scale Research Sector}
\gdef\KIT@G@BankD{Baden-W\"urttembergische Bank, Stuttgart}
\gdef\KIT@G@BankE{Baden-Wuerttembergische Bank, Stuttgart}
\gdef\KIT@G@Kto{BLZ~600~501~01 \vrule{} Kto.~7495501296}
\gdef\KIT@G@BIC{BIC:~SOLADEST}
\gdef\KIT@G@IBAN{IBAN:~DE18~6005~0101~7495~5012~96}
\gdef\LRS{%
  \KIT@Internfalse
  \colorlet{KITfoot}{KIT@foot}
  \ifKIT@De
    \let\KIT@Sect=\KIT@G@SectD
    \let\KIT@Bank=\KIT@G@BankD
  \else
    \let\KIT@Sect=\KIT@G@SectE
    \let\KIT@Bank=\KIT@G@BankE
  \fi
  \let\KIT@SectD=\KIT@G@SectD
  \let\KIT@BankD=\KIT@G@BankD
  \let\KIT@SectE=\KIT@G@SectE
  \let\KIT@BankE=\KIT@G@BankE
  \let\KIT@Kto=\KIT@G@Kto
  \let\KIT@BIC=\KIT@G@BIC
  \let\KIT@IBAN=\KIT@G@IBAN
}
\gdef\KIT{%
  \KIT@Internfalse
  \colorlet{KITfoot}{KIT@foot}
  \let\KIT@Sect=\relax
  \let\KIT@SectD=\relax
  \let\KIT@SectE=\relax
  \ifKIT@De
    \let\KIT@Bank=\KIT@U@BankD
  \else
    \let\KIT@Bank=\KIT@U@BankE
  \fi
  \let\KIT@BankD=\KIT@U@BankD
  \let\KIT@BankE=\KIT@U@BankE
  \let\KIT@Kto=\KIT@U@Kto
  \let\KIT@BIC=\KIT@U@BIC
  \let\KIT@IBAN=\KIT@U@IBAN
}
\KIT
\newif\ifKIT@Account\KIT@Accounttrue
\newcommand{\NoAccount}{\KIT@Accountfalse}
\newcommand{\Account}{\KIT@Accounttrue}
\let\KIT@PhoI=\relax
\newcommand{\PhoneInst}[1]{\gdef\KIT@PhoI{{#1}}}
\let\TelInst=\PhoneInst
\let\KIT@FaxI=\relax
\newcommand{\FaxInst}[1]{\gdef\KIT@FaxI{{#1}}}
\let\KIT@InstI=\relax
\newcommand{\Institution}[1]{\long\def\KIT@InstI{#1}}
\let\Einrichtung=\Institution
\let\KIT@FInst=\relax
\newcommand{\WInstitution}[1]{\gdef\KIT@FInst{#1}}
\let\FEinrichtung=\WInstitution
\let\KIT@IHd=\relax
\newcommand{\Head}[1]{\gdef\KIT@IHd{#1}}
\let\Leiter=\Head
\let\KIT@IHdTxt=\relax
\let\KIT@InstII=\relax
\newcommand{\Department}[1]{\gdef\KIT@InstII{{#1}}}
\let\Abteilung=\Department
\let\KIT@Geb=\relax
\newcommand{\Building}[1]{\gdef\KIT@Geb{{#1}}}
\let\Geb=\Building
\newif{\ifKIT@LA@}\KIT@LA@false
\newcommand{\DeliverAddr}{\KIT@LA@true}
\let\Lieferanschrift=\DeliverAddr
\let\KIT@Oic=\relax
\newcommand{\Official}[1]{\gdef\KIT@Oic{{#1}}}
\let\Bearbeiter=\Official
\let\KIT@OicTxt=\relax
\let\KIT@PhoD=\relax
\newcommand{\PhoneDirect}[1]{\gdef\KIT@PhoD{{#1}}}
\let\TelDurchwahl=\PhoneDirect
\let\KIT@Pho=\relax
\newcommand{\Phone}[1]{\gdef\KIT@Pho{{#1}}}
\let\Telefon=\Phone
\let\KIT@PhoTxt=\relax
\let\KIT@FaxD=\relax
\newcommand{\FaxDirect}[1]{\gdef\KIT@FaxD{{#1}}}
\let\FaxDurchwahl=\FaxDirect
\let\KIT@Fax=\relax
\newcommand{\Fax}[1]{\gdef\KIT@Fax{{#1}}}
\let\KIT@FaxTxt=\relax
\gdef\KIT@PrsTxtD{Pr\"asidenten}
\gdef\KIT@PrsTxtE{Presidents}
\gdef\KIT@VPrsTxtD{Vizepr\"asidenten}
\gdef\KIT@VPrsTxtE{Vice Presidents}
\gdef\KIT@IHdTxtD{Leiter/in}
\gdef\KIT@IHdTxtE{Head}
\gdef\KIT@OicTxtD{Bearbeiter/in}
\gdef\KIT@OicTxtE{Official in charge}
\gdef\KIT@RecTxtD{Empf\"anger}
\gdef\KIT@RecTxtE{Recipient(s)}
\gdef\KIT@PhoTxtD{Telefon}
\gdef\KIT@PhoTxtE{Phone}
\gdef\KIT@FaxTxtD{Fax}
\gdef\KIT@FaxTxtE{Fax}
\gdef\KIT@EmlTxtD{E-Mail}
\gdef\KIT@EmlTxtE{Email}
\gdef\KIT@DatTxtD{Datum}
\gdef\KIT@DatTxtE{Date}
\gdef\KIT@RefTxtD{Unser Zeichen}
\gdef\KIT@RefTxtE{Our Reference}
\gdef\KIT@LieferAnschrTxt{Lieferanschrift}
\gdef\KIT@GebTxt{Geb.}
\gdef\German{%
  \KIT@Detrue
  \KIT@internatfalse
  \let\KIT@PrsTxt=\KIT@PrsTxtD
  \let\KIT@VPrsTxt=\KIT@VPrsTxtD
  \let\KIT@IHdTxt=\KIT@IHdTxtD
  \let\KIT@OicTxt=\KIT@OicTxtD
  \let\KIT@RecTxt=\KIT@RecTxtD
  \let\KIT@PhoTxt=\KIT@PhoTxtD
  \let\KIT@FaxTxt=\KIT@FaxTxtD
  \let\KIT@EmlTxt=\KIT@EmlTxtD
  \let\KIT@RefTxt=\KIT@RefTxtD
  \let\KIT@DatTxt=\KIT@DatTxtD
  \KIT@German
  \let\KIT@Bank=\KIT@BankD
  \let\KIT@Sect=\KIT@SectD
  \let\KIT@BankL=\KIT@G@BankD
  \let\KIT@USt=\KIT@UStD
  \renewcommand{\enclname}{Anlagen(n)}
  \renewcommand{\ccname}{Verteiler}
  \renewcommand{\psname}{PS}
  \ifKIT@bw\let\KIT@logo=\KIT@logo@bwD
    \else\gdef\KIT@logo{\KIT@logo@clrD}
    \fi
}
\let\Deutsch=\German
\gdef\English{%
  \KIT@Defalse
  \KIT@internattrue
  \let\KIT@PrsTxt=\KIT@PrsTxtE
  \let\KIT@VPrsTxt=\KIT@VPrsTxtE
  \let\KIT@IHdTxt=\KIT@IHdTxtE
  \let\KIT@OicTxt=\KIT@OicTxtE
  \let\KIT@RecTxt=\KIT@RecTxtE
  \let\KIT@PhoTxt=\KIT@PhoTxtE
  \let\KIT@FaxTxt=\KIT@FaxTxtE
  \let\KIT@EmlTxt=\KIT@EmlTxtE
  \let\KIT@RefTxt=\KIT@RefTxtE
  \let\KIT@DatTxt=\KIT@DatTxtE
  \KIT@English
  \let\KIT@Bank=\KIT@BankE
  \let\KIT@Sect=\KIT@SectE
  \let\KIT@BankL=\KIT@G@BankE
  \let\KIT@USt=\KIT@UStE
  \renewcommand{\enclname}{encl}
  \renewcommand{\ccname}{cc}
  \renewcommand{\psname}{P.S.{}}
  \ifKIT@bw\let\KIT@logo=\KIT@logo@bwE
    \else\gdef\KIT@logo{\KIT@logo@clrE}
    \fi
}
\let\Englisch=\English
\newif\ifKIT@internat\KIT@internatfalse
\newcommand{\International}{\KIT@internattrue}
\newcommand{\InternationalD}{\let\KIT@Cntry=\KIT@CntryD\KIT@internattrue}
\newcommand{\InternationalE}{\let\KIT@Cntry=\KIT@CntryE\KIT@internattrue}
\newcommand{\National}{\KIT@internatfalse}
\paperwidth=210mm
\paperheight=297mm
\AtBeginPage{%
  \begin{tikzpicture}
    \useasboundingbox (0mm,297mm) rectangle (0mm,297mm);
    \ifKIT@grid
      \draw[step=1mm,style=help lines]
        (0mm,0mm) grid (\paperwidth,\paperheight);
      \draw[step=10mm,red,very thin]
        (0mm,0mm) grid (\paperwidth,\paperheight);
      \draw[blue,thin]
        (0mm,0mm) -- (\paperwidth,0mm) -- (\paperwidth,\paperheight)
                  -- (0mm,\paperheight) -- cycle;
    \fi
  \end{tikzpicture}}%
\newlength{\KIT@btlineht}
\newlength{\KIT@bttoplin}
\newlength{\KIT@btfntht}
\sbox{\KIT@boxii}{\fontsize{5.06pt}{6pt}\selectfont K}
\KIT@btfntht=\ht\KIT@boxii
\addtolength{\KIT@btfntht}{2.4mm}
\newlength{\KIT@FcolP}
\newlength{\KIT@FcolA}
\newlength{\KIT@FcolB}
\newlength{\KIT@tmplen}
\newcommand{\KIT@Kopf}{%
  \gdef\KIT@vsp{\rule{0pt}{\KIT@vsk}}%
  \vspace*{9.2mm}%
  \begin{tikzpicture}
    \useasboundingbox (20mm,285.9mm) rectangle (20mm,285.9mm);
      \draw[KITblack,line width=\KIT@RFramewd]
        (3.5mm,87mm) -- (6.5mm,87mm) (3.5mm,192mm) -- (6.5mm,192mm);
    \ifKIT@RFrame
        \KIT@MFlly=19.5mm
      \KIT@MFrameRA
      \pgftext[at=\pgfpoint{20mm}{281mm},left,top]
        {\includegraphics[width=40.2mm]{\KIT@logo}}
      \ifKIT@Intern
        \relax
      \else
        \pgftext[at=\pgfpoint{20mm}{13.0mm},left,base]
          {\fontsize{6pt}{8pt}\selectfont\KIT@Text}
        \pgftext[at=\pgfpoint{197.5mm}{13.0mm},right,base]
          {\fontsize{15pt}{18pt}\bfseries\color{KITblack}\KIT@Web}
        \setlength{\KIT@btlineht}{19.5mm+12.6mm+2.4mm}
        \setlength{\KIT@bttoplin}{\KIT@btlineht-\KIT@btfntht}
      \fi
    \else
      \setlength{\KIT@btlineht}{19.5mm+12.6mm+2.4mm-6mm}
      \setlength{\KIT@bttoplin}{\KIT@btlineht-\KIT@btfntht}
      \enlargethispage{6mm}
    \fi
    \pgftext[at=\pgfpoint{130mm}{272.5mm},left,top]{\usebox{\KIT@box}}
    \ifKIT@Intern
      \relax
    \else
      \pgftext[at=\pgfpoint{20mm}{249mm},left,top]
        {\fontsize{7pt}{8.5pt}\selectfont\KIT@Abs
         \ifKIT@internat, \KIT@Cntry\fi}
    \fi
      \pgftext[at=\pgfpoint{20mm}{227mm},left]
        {\parbox{85mm}{\raggedright\@addresslist}}
    \ifKIT@Intern
      \relax
    \else
      \draw[KITblack,line width=\KIT@RFramewd]
        (20mm,\KIT@btlineht) -- (190mm,\KIT@btlineht);
      \fontsize{5.06pt}{6pt}\selectfont
      \pgftext[at=\pgfpoint{20mm}{\KIT@bttoplin},left,base]
        {\vphantom{Eg}\KIT@Name}
      \if\KIT@Sect\relax
        \pgftext[at=\pgfpoint{20mm}{\KIT@bttoplin-7pt},left,base]
          {\vphantom{Eg}\KIT@Street}
        \pgftext[at=\pgfpoint{20mm}{\KIT@bttoplin-14pt},left,base]
          {\vphantom{Eg}\KIT@Place}
        \KIT@FcolP=57.5mm
        \KIT@FcolA=115.5mm
        \KIT@FcolB=152.5mm
      \else
        \pgftext[at=\pgfpoint{20mm}{\KIT@bttoplin-7pt},left,base]
          {\vphantom{Eg}\KIT@Sect}
        \pgftext[at=\pgfpoint{20mm}{\KIT@bttoplin-14pt},left,base]
          {\vphantom{Eg}\KIT@Street}
        \pgftext[at=\pgfpoint{20mm}{\KIT@bttoplin-21pt},left,base]
          {\vphantom{Eg}\KIT@Place}
        \KIT@FcolP=69.5mm
        \KIT@FcolA=140.5mm
      \fi
      \pgftext[at=\pgfpoint{\KIT@FcolP}{\KIT@bttoplin},left,base]
        {\vphantom{Eg}\KIT@PrsTxt: \KIT@Prs}
      \pgftext[at=\pgfpoint{\KIT@FcolP}{\KIT@bttoplin-7pt},left,base]
        {\vphantom{Eg}\KIT@VPrsTxt: \KIT@VPra}
      \pgftext[at=\pgfpoint{\KIT@FcolP}{\KIT@bttoplin-14pt},left,base]
        {\vphantom{Eg}\KIT@VPrb}
      \ifKIT@Account
        \pgftext[at=\pgfpoint{\KIT@FcolA}{\KIT@bttoplin},left,base]
          {\vphantom{Eg}\KIT@Bank}
        \pgftext[at=\pgfpoint{\KIT@FcolA}{\KIT@bttoplin-7pt},left,base]
          {\vphantom{Eg}\KIT@Kto}
        \pgftext[at=\pgfpoint{\KIT@FcolA}{\KIT@bttoplin-14pt},left,base]
          {\vphantom{Eg}\KIT@BIC}
        \pgftext[at=\pgfpoint{\KIT@FcolA}{\KIT@bttoplin-21pt},left,base]
          {\vphantom{Eg}\KIT@IBAN}
        \pgftext[at=\pgfpoint{\KIT@FcolA}{\KIT@bttoplin-28pt},left,base]
          {\vphantom{Eg}\KIT@USt}
        \if\KIT@Sect\relax
          \pgftext[at=\pgfpoint{\KIT@FcolB}{\KIT@bttoplin},left,base]
            {\vphantom{Eg}\KIT@BankL}
          \pgftext[at=\pgfpoint{\KIT@FcolB}{\KIT@bttoplin-7pt},left,base]
            {\vphantom{Eg}\KIT@G@Kto}
          \pgftext[at=\pgfpoint{\KIT@FcolB}{\KIT@bttoplin-14pt},left,base]
            {\vphantom{Eg}\KIT@G@BIC}
          \pgftext[at=\pgfpoint{\KIT@FcolB}{\KIT@bttoplin-21pt},left,base]
            {\vphantom{Eg}\KIT@G@IBAN}
        \fi
      \fi
    \fi
  \end{tikzpicture}%
}
\AtBeginDocument{%
\addtolength{\oddsidemargin}{2.5mm}
\addtolength{\evensidemargin}{-2.5mm}
  \if\KIT@InstI\relax%
    \InputIfFileExists{inst.tex}{}{}
    \if\KIT@InstI\relax
      \message{Eine Einrichtung/Institution muss angegeben werden mit einem der  folgenden Befehle - an institution must be specified using one of the following commands: \string\Einrichtung\{...\} oder/or \string\Institution\{...\}}
    \fi
  \fi
  \address{\KIT@Kopf}
  \pagestyle{KIT}
  \@reflinefalse
  \let\KIT@db@open=\opening
  \gdef\opening{%
    \ifKIT@Intern
      \enlargethispage{-6mm}%
    \else
      \enlargethispage{-12mm}%
    \fi
    \KIT@db@open
  }
}
\gdef\ka@db@placetoaddress{\vspace*{\KIT@AddrHt}}
\DateText
\German
\AtBeginDocument{\KIT@cmod}
\endinput
%%
%% End of file `KITbrief.cls'.

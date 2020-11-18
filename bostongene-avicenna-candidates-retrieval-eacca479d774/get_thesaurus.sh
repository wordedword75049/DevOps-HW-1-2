#!/bin/bash

curl http://evs.nci.nih.gov/ftp1/NCI_Thesaurus/Thesaurus_20.08e.OWL.zip -L -o Thesaurus.zip

unzip Thesaurus.zip

python3.7 -c "from complex_script import deThesaurusify; deThesaurusify('./Thesaurus.owl', 'text_thesaurus')"

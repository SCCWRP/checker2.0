library(gsw)
library(oce)
library(SolveSAPHE)
library(seacarb)
library(phcorrection)
library(readxl)
library(openxlsx)

args = commandArgs(trailingOnly=TRUE)

dir <- args[1]
filename <- args[2]

wbpath <- file.path(dir, filename)

ctd <- readxl::read_excel(wbpath, sheet = 'tbl_oactd')
bottle <- readxl::read_excel(wbpath, sheet = 'tbl_oabottle')

# Run the pH correction function
# Calculates delta pH and corrects, and gets the omega aragonite saturation (hopefully i said that right)
output <- phcorrection::ph.omega(ctd, bottle)

write.csv(output$ctd, file = file.path(dir, 'analysis_ctd.csv'), row.names = FALSE)
write.csv(output$bottle, file = file.path(dir, 'analysis_bottle.csv'), row.names = FALSE)
######### merging of classification results
#merges all subsets into one raster for the tile
#merges classified tile with training data, giving priority to training data
#deletes the folders with original scene data
#########

rm(list = ls())

dataIn <- "../data/inputData"
rfIn <- "../data/RFResults"
mergeIn <- "../data/MergeResults"

###
# command line args
###
args <- commandArgs(TRUE)

if (length(args) == 0) {
  stop("Tile name must be specified!!", call.=FALSE)
}
tile <- args[1] #add consistency checks later
###

tileIn <- paste0(dataIn, "/", tile)
subsetIn <- paste0(tileIn, "/subsetResults")

lc <- paste0(tile, "_lc_UTM.tif")
tl <- paste0(rfIn, "/", tile, "_tile.tif")
mg <- paste0(mergeIn, "/", tile, "_merge.tif")


#first merge RF result
setwd(subsetIn)
fl <- paste(list.files(pattern=tile), collapse = " ")
system(paste("gdal_merge.py", fl, "-o", tl))
print(paste(tile, "subsets merging done"))

#then merge RF result tile with OSM landuse
setwd(tileIn)
system(paste("gdal_translate -of GTiff -a_nodata 0", lc, "tout.tif"))
system(paste("mv", lc, "old_lc.tif"))
system(paste("mv tout.tif", lc))
system(paste("gdal_merge.py", tl, lc, "-co compress=LZW -o", mg))
print(paste(tile, "final merging done"))

#now delete dirs
dl<- dir(pattern="*.SAFE", recursive=F)
for (i in 1:length(dl)) {
  system(paste("rm -r", dl[i]))
}

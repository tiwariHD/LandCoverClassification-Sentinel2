######## Download scenes script
# Downloads scenes for tiles by extracting names from outnames.txt
# Data downloaded in tempData
# For now manual move required for tiles from tempData to ../data/inputData
# Only 2 parallel downloads possible (api limitation)
# Tiles which are already downloaded (present in folder completedTiles) are not downloaded again
########

rm(list = ls())

library(doMC)
registerDoMC(2)
#for each user using command line tool only 2 concurrent downloads are allowed, hence using only 2 cores

completedOut <- "../data/completedTiles"
dataOut <- "./tempData"
scenesData <- read.table("./outnames.txt")
tile <- substr(scenesData$V6, regexpr("_T", scenesData[1,6])[1] + 2, regexpr("_T", scenesData[1,6])[1] + 6)
scenesData$tile <- tile


allTiles <- unique(scenesData$tile)
allTiles <- sort(allTiles)

#create a list with all completed tiles, and skip download if already present
completedTiles <- list.dirs(path = completedOut, full.names=F, recursive=F)

start <- Sys.time()
print(paste("----All download started:", start))

#change the value of the loop if do not want to download all of the tiles at once
#for (i in 61:length(allTiles)) {
for (i in 1:length(allTiles)) {
  tile <- allTiles[i]

  if (is.element(tile, completedTiles)) {
    print(paste("----", tile, "already completed, skipping it!"))
    next
  }

  start1 <- Sys.time()
  print(paste("----", tile, "download started:", start1))
  foreach (j = 1:nrow(scenesData)) %dopar% {
    if (scenesData$tile[j] == tile) {
      print(paste("Line", j, "downloading.."))
      system(paste("eval `sed \"", j, "q;d\" outnames.txt`"), intern=FALSE, ignore.stdout=TRUE, wait=TRUE)
    }
  }

  system(paste("mkdir", tile))
  system(paste("mv *.zip", tile))
  system(paste("mv", tile, dataOut))

  end1 <- Sys.time()
  print(paste("----", tile, "all scenes dowloaded and moved", end1))
  print(end1 - start1)
}

end <- Sys.time()
print(paste("----All tiles dowloaded", end))
print(end - start)

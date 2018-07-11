###code for deleting scenes, in tiles where RF did not run

#path for head directory containing the tiles to be cleared
dataPath <- "../inputData"
setwd(dataPath)

dl <- list.dirs(recursive=F) 
for (j in dl){
  setwd(j)
  fl<- dir(pattern="*.SAFE", recursive=F)
  if (length(fl) != 0) {
    print(paste("Deleting", length(fl), "directories in ", j))
    for (i in 1:length(fl)) {
      system(paste("rm -r", fl[i]))
    }
  }
  setwd(dataPath)
}

######## preprocessing scenes for Sentinel-2
#takes tile name as command line argument
#creates one input data brick from the scenes
#and one training data raster from osmlu database dump
########

rm(list = ls())

library(raster) # raster funcionalities
library(rgdal) # gdal funcionalities
library(doMC)
registerDoMC(6)

# data base access specific
library(RPostgreSQL)
library(postGIStools)

In <- "../data/inputData"
tmp <- "../data/temp"  #temp dir
rasterOptions(tmpdir=tmp) # set temp dir

###
# clearing temp dir
###
clearTemp <- function(){
  setwd(tmp)
  fl <- list(list.files(recursive=T, full.names=T))
  if (length(fl) != 0) {
    do.call(file.remove, fl)
  }
}

###
# command line args
###
args <- commandArgs(TRUE)
unzip <- TRUE

if (length(args) == 0) {
  stop("Tile name must be specified!!", call.=FALSE)
}

tile <- args[1] #add consistency checks later

if (length(args) >= 2) {
  if (args[2] == "FALSE") {
    unzip <- FALSE
  }
}

###
# Within each tile do preprocessing
#  create brick for input data from sentinel scenes
#  create raster for training data from osm database
###
#tile <- c("32UMB") #testing for now, removing unzipping for now
bl <- c("_B02_10m", "_B03_10m", "_B04_10m", "_B08_10m") # relevant band list
su <- "_sub" # suffix

InTile <- paste0(In, "/", tile)
start <- Sys.time()
print(paste("----", tile, "tile started:", start))

###
# Unzip scenes in tiles
# also deletes the zip files after unzipping
###
if (unzip) {
  setwd(InTile)
  filesT <- list.files(pattern = ".zip")
  print(paste("Tile:", tile, "No. of scenes:", length(filesT)))
  foreach(j = 1:length(filesT)) %dopar% {
    fT <- filesT[j]
      if (length(unzip(fT)) == 0) {
      print(paste("Cannot unzip", fT))
    } else { #delete the zip files
      file.remove(fT)
      print(paste(j, "scene unzipped & zip deleted:", Sys.time()))
    }
  }
  print(paste("----scenes unzipped:", Sys.time()))
} else {
  print(paste("----unzipping skipped"))
}

setwd(InTile)
sceneDirs <- list.dirs(full.names = F, recursive = F) # list folders
print(paste("----masking of rasters started"))

#create masked rasters
foreach(i = 1:length(sceneDirs)) %dopar% {
  
  setwd(paste0(InTile, "/", sceneDirs[i]))
  
  # prepare quality mask
  maskName <- list.files(pattern = "*_SCL_20m.jp2", recursive = T, full.names = T)[1]
  outMaskName <- paste0(substr(maskName, 0, nchar(maskName)-7), "10m.tif")
  # ix <- try(system(paste("gdalwarp -tr 10 10", maskName, outMaskName), intern=FALSE, ignore.stdout=TRUE)) #error will be 0 length
  system(paste("gdalwarp -tr 10 10", maskName, outMaskName), intern=FALSE, ignore.stdout=TRUE, wait=TRUE)
  x <- raster(list.files(pattern = "*_SCL_10m.tif", recursive = T, full.names = T)[1]) # 10 m classification mask
  #print(paste(i, "resample done", Sys.time()))
  
  x <- reclassify(x, matrix(c(0,NA, # no data
	                      1,NA, # saturated or defective
	                      2,NA, # dark area pixels
	                      3,NA, # cloud shadows
	                      7,NA, # cloud low probability
	                      8,NA, # cloud medium probability
	                      9,NA, # cloud high probability
	                      10,NA, # thin clouds
	                      11,NA, # snow
	                      4, 1, # vegetation
	                      5, 1, # bare
	                      6, 1), ncol=2, byrow=T)) # water
  
  #print(paste(i, "reclassify done"))
  
  for (j in 1:length(bl)){ # apply to each raster
    bn <- list.files(pattern = bl[j], recursive = T, full.names = T) # band name
    b <- raster(bn) # declare as raster
    temp <- overlay(x, b, fun=function(a,b){return(a*b)}, filename=paste0(substr(bn, 0, nchar(bn)-4), su), format="GTiff", datatype="INT2S", overwrite=T)
    
  }
  #print(paste(i, "scene done:", sceneDirs[i], Sys.time()))
}

print(paste("----masking of rasters done", Sys.time()))

setwd(InTile) # work dir

foreach (j = 1:length(bl)) %dopar% {
  
  fl <- list.files(pattern=paste0(bl[j], su), recursive = T, full.names=T) # call cleaned bricks
  
  # create bands mosaic
  
  fl <- lapply(fl, raster) # create list of bricks
  fl$na.rm <- T # ignore NA values from analysis
  fl$fun <- max # mosaic function
  x <- do.call(mosaic, fl) # create mosaic
  temp <- writeRaster(x, filename=paste0(tile, bl[j]), format="GTiff", datatype="INT2S", overwrite=T) # output classification
  
}

print(paste("----mosaicing of rasters done", Sys.time()))

# create brick
setwd(InTile) # work dir

fl <- list.files(pattern="10m.tif", recursive = F, full.names=F) # call fused mosaics
fl <- lapply(fl, raster) # list of raster files
b <- do.call(brick, fl) # create brick
b[is.na(b)] <- 0 # fill NA with 0

name <- paste0(tile, "_brick")
temp <- writeRaster(b, filename=name, format="GTiff", datatype="INT2S", overwrite=T) # output classification

print(paste("----", name, "written to output:", Sys.time()))


#creating training raster now

###
# Extracting training labels from OSM landuse database dump
# change username password
##
conn <- dbConnect(dbDriver("PostgreSQL"), dbname="osm-lulc",host="localhost",
        port=5432, user="****", password="****") # open db

print(paste("---- Now creating training raster"))
e <- as(extent(b), "SpatialPolygons") # create polygon from tile extent
proj4string(e) <- projection(b) # polygons projection as tiles

e <- spTransform(e, CRS("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs")) # convert to data bases WGS84
ext <- c(xmin(extent(e)), ymin(extent(e)), xmax(extent(e)), ymax(extent(e))) # create db conform extent

sq <- paste0("SELECT * from lulc_europe_para where the_geom && 
  st_transform(st_makeenvelope(", paste0(ext[1], ", ", ext[2],", ",
  ext[3],", ", ext[4]), ", 4326), 3857)") # querry tiles lu
p <- get_postgis_query(conn, sq, geom_name = "the_geom") # get lu polygon
p <- p[,-(1:3)] # remove columns 1 to 3
print(paste("----training shapefile extractedfrom db", Sys.time()))

# format tiles lc data
p <- spTransform(p, CRS(projection(b))) # convert to local UTM projection
print(paste("----transformation done", Sys.time()))

#write shapefile here
temp <- writeOGR(obj=p, dsn=InTile, layer=tile, driver="ESRI Shapefile")
print(paste("----", tile, "shapefile created, now rasterizing"))

shp_var <- readOGR(dsn=InTile, layer=tile)
extB <- extent(b)
system(paste("gdal_rasterize -a", names(shp_var)[1], "-te", extB[1], extB[3], extB[2], extB[4], "-tr 10 10 -a_nodata 0 -ot Int16",
paste0(tile, ".shp"), paste0(tile, "_lc_UTM.tif")), intern=FALSE, ignore.stdout=TRUE, wait=TRUE)
print(paste("----rasterization done", Sys.time()))

end <- Sys.time()
print(paste("----", name, "written to output:", end))
print(end - start)


dbDisconnect(conn) # close connection

clearTemp() # delete contents of temp

###
# Random Forest classification script for Landuse classification
# Training data: Sentinel-2 tiles
# Training labels: OSM Landuse data with missing labels
# Output: Raster with all the gaps classified
###

from osgeo import gdal, gdal_array
from osgeo import ogr
from osgeo import gdalconst
from osgeo import osr

import numpy as np
from sklearn.ensemble import RandomForestClassifier

from datetime import datetime
import os
import logging
import resource
import subprocess
from multiprocessing import Process


###
# Globals
###
PRE_LOG_DIR = "../logs/preLogs" #for preprocessing logs
DATA_DIR = "../data/inputData" # input Sentinel-2 bricks
LOG_DIR = "../logs/subsetLogs"  # logs for RandomForest

TILE_SIZE = 5490
NUM_RF_TREES = 100
NUM_RF_CORES = 10
RF_TIMEOUT = 36000 # timeout of 10 hours for RF processes

PRESCRIPT = "./preprocess_tiles_gdal.R"
MERGESCRIPT = "./result_merge.R"

###
# Function
# creates a logger object for a file
###
def setupLogger(name, level=logging.INFO):

    logFile = LOG_DIR + "/" + name + ".log"
    handler = logging.FileHandler(logFile, mode="w")
    formatter = logging.Formatter("%(asctime)s %(message)s")   
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


###
# Function
# creates subsets and returns file names
###
def subsetUTM(fileName, dirName, tileSize = 500, pattern = "", fileFormat = "GTiff"):
    
    geoFile = gdal.Open(fileName, gdalconst.GA_ReadOnly)
    cols = geoFile.RasterXSize
    rows = geoFile.RasterYSize
    colList = np.arange(0, cols, tileSize)
    rowList = np.arange(0, rows, tileSize)
    
    rowOffset = np.ones(rowList.size) * tileSize
    rowOffset[-1] = rows - rowList[-1]
    if (rowOffset[-1] == 0):
        rowList = np.delete(rowList, rowList.size-1)
    
    colOffset = np.ones(colList.size) * tileSize
    colOffset[-1] = cols - colList[-1]
    if (colOffset[-1] == 0):
        colList = np.delete(colList, colList.size-1)
        
    subsetNames = []
    
    for i in range(0, rowList.size):          
        for j in range(0, colList.size):
            subName = dirName + "_" + str(i) + "." + str(j) + pattern  + ".tif"
            subsetNames.append(subName)
            subprocess.call(["gdal_translate", "-srcwin", str(colList[j]), str(rowList[i]), str(colOffset[j]),
                str(rowOffset[i]), fileName, subName])
            
    return subsetNames


###
# Function
# calls Random forest and outputs predicted raster
###
def RandomForest(rawImgName, trainImgName, outPath = ".", outFile = "result"):

    rfLog = setupLogger(outFile)
    resultFile = outPath + "/" + outFile + ".tif"

    rfLog.info("Preparing data...")

    # read training image data
    trainImg = gdal.Open(trainImgName, gdalconst.GA_ReadOnly)
    trainData = trainImg.GetRasterBand(1).ReadAsArray()
    rfLog.info("TrainData, Shape: %s , Size: %s ", str(trainData.shape), str(trainData.size))

    if (len(np.unique(trainData)) < 2):
        rfLog.info("No of training classes < 2, will not run RF for this tile!")
        return

    # read raw image data
    rawImg = gdal.Open(rawImgName, gdalconst.GA_ReadOnly)
    rawDataTemp = rawImg.GetRasterBand(1).ReadAsArray()

    if (len(np.unique(rawDataTemp)) < 2):
        rfLog.info("Unique input values < 2, will not run RF for this tile!")
        return
  
    rawData = np.zeros((rawImg.RasterYSize, rawImg.RasterXSize, rawImg.RasterCount),
                   gdal_array.GDALTypeCodeToNumericTypeCode(rawImg.GetRasterBand(1).DataType))
    
    for b in range(rawImg.RasterCount):
        rawData[:, :, b] = rawImg.GetRasterBand(b+1).ReadAsArray()
    rfLog.info("RawData, Shape: %s , Size: %s ", str(rawData.shape), str(rawData.size))
    
    # set NA data to 0
    trainData[trainData < 0] = 0
    rawData[rawData < 0] = 0

    # clean labels, convert to non-negative int
    trainData = trainData.astype(int)

    # preprocess data, labels, find all data which is nonzero in both raw and training image
    rfLog.info("Fetching train sample list")
    rowT = []
    colT = []
    count = 0
    for i in xrange(0, trainData.shape[0]):
        for j in xrange(0, trainData.shape[1]):
            if (0 not in rawData[i, j] and trainData[i, j] != 0):
            # if (not all(rawData[i, j] == 0) and trainData[i, j] != 0):
                rowT.append(i)
                colT.append(j)
            # also introduce few 0 label data
            elif (count < 1000):
                if (all(rawData[i, j] == 0) and trainData[i, j] == 0):
                    rowT.append(i)
                    colT.append(j)
                    count = count + 1

    #trainIdx = np.nonzero(trainData)
    trainIdx = (rowT, colT)
    X_train = rawData[trainIdx]
    Y_train = trainData[trainIdx]

    rfLog.info("train_labels - num_samples")
    labels = sorted(list(np.unique(Y_train)))
    for l in labels:
        rfLog.info("%s - %s", str(l), str((Y_train == l).sum()))
    rfLog.info("Y_train, Shape: %s , Size: %s ", str(Y_train.shape), str(Y_train.size))

    print("\tCreating RF model, memUsage: {} MB".format(memUsage())) #--for testing

    rfLog.info("Creating RF model..., memUsage: %s MB", memUsage())
    # create model
    # if njobs = -1, then the number of parallel jobs is set to the number of cores.
    # set verbose=100 for more logs
    rf_model = RandomForestClassifier(n_jobs=NUM_RF_CORES, n_estimators=NUM_RF_TREES, min_samples_leaf=50, oob_score=True)
    rf_model.fit(X_train, Y_train)
    rfLog.info("Model created, nTrees: %s, memUsage: %s MB", str(NUM_RF_TREES), memUsage())

    print("\tRF model created, nTrees: {}, memUsage: {} MB".format(NUM_RF_TREES, memUsage())) #--for testing

    #performance score of training dataset	
    rfLog.info("oob score: " + str(rf_model.oob_score_))

    # classify raw image
    rfLog.info("Classifying raw image...")
    rawFlat = rawData.reshape((rawData.shape[0]*rawData.shape[1], rawData.shape[2]))
    result = rf_model.predict(rawFlat)
    class_pred = result.reshape((rawData.shape[0], rawData.shape[1]))
    rfLog.info("Classification finished.")

    print("\tRF model classified, memUsage: {} MB".format(memUsage())) #--for testing

    rfLog.info('test_labels - num_samples')
    labels = sorted(list(np.unique(class_pred)))
    for l in labels:
        rfLog.info("%s - %s", str(l), str((class_pred == l).sum()))

    # write result to raster
    driver = trainImg.GetDriver()
    geotransform = trainImg.GetGeoTransform()
    originX = geotransform[0]
    originY = geotransform[3]
    pixelWidth = geotransform[1]
    pixelHeight = geotransform[5]
    outRaster = driver.Create(resultFile, class_pred.shape[1], class_pred.shape[0], 1, trainImg.GetRasterBand(1).DataType)
    outRaster.SetGeoTransform((originX, pixelWidth, 0, originY, 0, pixelHeight))
    outband = outRaster.GetRasterBand(1)
    outband.SetNoDataValue(-9999)
    outband.WriteArray(class_pred)
    outRasterSRS = osr.SpatialReference()
    outRasterSRS.ImportFromWkt(trainImg.GetProjectionRef()) #changed here
    outRaster.SetProjection(outRasterSRS.ExportToWkt())
    outband.FlushCache()
    rfLog.info("Output written to file: " +  resultFile)

    trainImg = None
    rawImg = None

    rfLog.info("END of RF, memUsage: %s MB", memUsage())


###
# Function
# return memory usage of current process in MB
###
def memUsage():

    return str(round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024.0,1))


###
# Function
# main
###
def main():

    gdal.UseExceptions()  # this allows GDAL to throw Python Exceptions
    os.chdir(DATA_DIR)

    #rawDirList = ["32UMU", "32UNU"] # changed tiles here and remove False later
    rawDirList = sorted(next(os.walk("."))[1])  # list of subdirectories (bricks)

    print("--No of directories (hopefully all bricks): ", len(rawDirList))

    for rwd in rawDirList:

        currentDir = DATA_DIR + "/" + rwd
        trainFileName = currentDir + "/" + rwd + "_lc_UTM.tif"
        rawFileName = currentDir + "/" + rwd + "_brick.tif"
        rLogName = PRE_LOG_DIR + "/" + rwd + "_rLog.txt"
        subsetOutDir = currentDir + "/subsetResults"

        print("----Calling preprocessing script for: {}".format(rwd))
        with open(rLogName,'w') as fileobj:
            if subprocess.call(["Rscript", "--vanilla", PRESCRIPT, rwd], stdout=fileobj, stderr=subprocess.STDOUT):
                print("Error in preprocessing, skipping tile {} !!".format(rwd))
                continue
            else:
                print("Preprocessing done, {} written".format(rLogName))

        try:
            os.chdir(currentDir)
            print("----Starting Brick: {}, Creating subsets now...".format(rwd))
            rawSubNames = subsetUTM(rawFileName, rwd, TILE_SIZE, pattern = "_sub")
            trainSubNames = subsetUTM(trainFileName, rwd, TILE_SIZE , pattern = "_tr")
        except Exception as e:
            print(e)
            print("Exiting {} now, will continue with next brick if in loop !!".format(rwd))
            continue

        print("----No of subsets created: ", len(rawSubNames))

        #create an ouput folder for subset results
        if not os.path.exists(subsetOutDir):
            os.makedirs(subsetOutDir)

        #now run random forest on the tiles, by starting a child process for every tile
        for i in range(len(rawSubNames)):

            try:
                outFile = rawSubNames[i].split("_sub")[0]
                print("\tRunning subset: {}, name: {}, memUsage: {} MB".format(i, outFile, memUsage()))
                p = Process(target=RandomForest, args=(rawSubNames[i], trainSubNames[i], subsetOutDir, outFile))
                p.start()
                p.join(timeout=RF_TIMEOUT)
                if (p.is_alive()):
                    print("Timeout (10 hours), now terminating ", outFile)
                    p.terminate()
            except Exception as e:
                print(e)
                print("Exiting {} now, will continue with next tile if in loop !!".format(rawSubNames[i]))
                continue

        #now call merge script
        print("----Calling merge result script for: {}".format(rwd))
        if subprocess.call(["Rscript", "--vanilla", MERGESCRIPT, rwd]):
            print("Error in merging result for tile {} !!".format(rwd))
        else:
            print("Merging for {} done".format(rwd))


###
# Run main script
###
if __name__ == "__main__":

    start = datetime.now()
    print("***Starting main script: {} ***".format(start))
    
    main()

    end = datetime.now()
    print("***Main script finished: {} ***".format(end))
    print("Total Runtime: (hh:mm:ss.ms) {}".format(end - start))


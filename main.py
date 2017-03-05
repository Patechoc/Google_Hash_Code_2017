#! /usr/bin/env python
# -*- coding: utf-8 -*-

""" 
Hash Code 2017
"""

import os, sys
from pprint import pprint
import argparse
import time
import datetime
import pandas as pd
import numpy as np
import scipy.optimize as spo
from pymongo import MongoClient
import simulated_annealing
import caching


def read_inputs():
    ### Read inputs from command line (path to images)
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--input_file",
                        type=lambda x: is_valid_file(parser, x),
                        required=True,
                        help="Enter a path to the input file describing the cache servers, network endpoints and videos, along with predicted requests for individual videos")
    parser.add_argument("-o", "--output_file",
                        help="Enter a path to the output file describing the distribution of videos in the cache servers.")
    args = parser.parse_args()
    inFile = os.path.basename(args.input_file.name)
    if args.output_file != None:
        outFile = args.output_file
    else:
        filename, file_extension = os.path.splitext(inFile)
        outFile = filename + ".out"
    lines_original = args.input_file.readlines()
    lines = [l.replace("\n","") for l in lines_original]
    return (lines, inFile, outFile)



def is_valid_file(parser, arg):
    if not os.path.exists(arg):
        parser.error("The file %s does not exist!" % arg)
    else:
        return open(arg, 'r')  # return an open file handle

def test():
    ## Read inputs
    (lines, inFile, outFile) = read_inputs()

    ## Build useful data structures (mainly arrays and matrices)
    (nb_videos, video_sizes, nb_endpoints, nb_requestsDesc,
     nb_caches, nb_MBperCache, endpoints, endpoints_caches,
     videos_endpoints, nb_videos_ranked_not_null, idx_videos_ranked) = caching.structure_inputs(lines)

    ## limit our study to the videos actually on demands
    videoIDs_to_consider = idx_videos_ranked[:nb_videos_ranked_not_null]


    ## initialization of the cache servers: random dispatch of videos within the caches
    videoIDs_in_caches = []
    for c in xrange(nb_caches):
        #videoIDs_in_caches.append([])
        # videoIDs_in_caches.append([videoIDs_to_consider[0],
        #                            videoIDs_to_consider[1],
        #                            videoIDs_to_consider[2],
        #                            videoIDs_to_consider[3]])
        videoIDs_in_caches.append([videoIDs_to_consider[0]])
    videoIDs_in_caches = [[2],[3,1],[0,1]]
        
    ## check that the configuration of videos in cache is possible (not exceedign cache size)
    isFittingCacheSize = caching.check_video_subset_fit_in_caches(videoIDs_in_caches, video_sizes, nb_MBperCache)
    if not(isFittingCacheSize):
        msg = "Set of videoIDs_in_caches not always fitting cache size !!!!"
        print "#"*len(msg)
        print msg
        print "#"*len(msg)
        return
    
    ## compute the score for a given set of videos within caches
    score_arbitrary_choice = caching.get_score(videoIDs_in_caches, videos_endpoints, endpoints, endpoints_caches)
    print("New score after arbitrary dispatch of videos")
    print score_arbitrary_choice
    caching.writing_videos_in_caches(videoIDs_in_caches, outFile=outFile)


def main():
    ## Read inputs
    (lines, inFile, outFile) = read_inputs()

    ## Build useful data structures (mainly arrays and matrices)
    (nb_videos, video_sizes, nb_endpoints, nb_requestsDesc,
     nb_caches, nb_MBperCache, endpoints, endpoints_caches, mostConnected_caches,
     videos_endpoints, nb_videos_ranked_not_null, idx_videos_ranked) = caching.structure_inputs(lines)
    
    ## OPTIMIZE
    videoIDs_in_caches = caching.solve_with_common_sense(endpoints, videos_endpoints,
                                                         idx_videos_ranked, nb_videos_ranked_not_null,
                                                         video_sizes,nb_MBperCache, nb_caches, endpoints_caches,
                                                         mostConnected_caches, outFile=outFile)

    ## check that the configuration of videos in cache is possible (not exceedign cache size)
    isFittingCacheSize = caching.check_video_subset_fit_in_caches(videoIDs_in_caches, video_sizes, nb_MBperCache)
    if not(isFittingCacheSize):
        msg = "Set of videoIDs_in_caches not always fitting cache size !!!!"
        print "#"*len(msg)
        print msg
        print "#"*len(msg)
        return

    score_common_sense = caching.get_score(videoIDs_in_caches, videos_endpoints, endpoints, endpoints_caches)
    print("New score after common sense dispatch of videos")
    print score_common_sense


    ## WRITE OUTPUT
    caching.writing_videos_in_caches(videoIDs_in_caches, outFile=outFile)

    
if __name__ == "__main__":
    main()


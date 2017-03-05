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
from pymongo import MongoClient
import numpy as np
from math import cos, sin
from collections import defaultdict, deque
import copy
import matplotlib.pyplot as plt
from sets import Set

def structure_inputs(lines):
    ## NETWORK
    ## 5 videos, 2 endpoints, 4 request descriptions, 3 caches 100MB each.
    network = lines.pop(0).split(" ")
    nb_videos = int(network[0])
    nb_endpoints = int(network[1])
    nb_requestsDesc = int(network[2])
    nb_caches = int(network[3])
    nb_MBperCache = int(network[4])
    print "nb_videos:", nb_videos
    print "nb_endpoints:", nb_endpoints
    print "nb_requestsDesc:", nb_requestsDesc
    print "nb_caches:", nb_caches
    print "nb_MBperCache:", nb_MBperCache
    
    ## VIDEOS
    video_sizes_row = lines.pop(0)
    video_sizes = [int(s) for s in video_sizes_row.split(" ")]
    print("\nVideos Sizes")
    pprint(video_sizes)
    
    
    ## ENDPOINTS (with caches)
    endpoints = []
    endpoints_caches =  np.ones((nb_endpoints, nb_caches), dtype=np.int) * -1
    for e in xrange(nb_endpoints):
        infosE = lines.pop(0).split(" ")
        endpoints.append({})
        endpoints[e]['id'] = e
        endpoints[e]['Ld'] = int(infosE[0])
        endpoints[e]['nb_caches'] = int(infosE[1])
        endpoints[e]['caches'] = []
        for c in xrange(endpoints[e]['nb_caches']):
            infosEC = lines.pop(0).split(" ")
            cacheID = int(infosEC[0])
            cacheLatency = int(infosEC[1])
            endpoints[e]['caches'].append(cacheID)
            endpoints_caches[e][cacheID] = cacheLatency
    print("\nEndpoints")
    pprint(endpoints)
    print("\nEndpoints x Caches latency matrix")
    print endpoints_caches

    ## ENDPOINTS improved: re-order caches of endpoints from fastest to slowest
    for e in xrange(nb_endpoints):
        cacheIDs = np.array(copy.copy(endpoints[e]['caches']))
        cacheLatencies = []
        for c in cacheIDs:
            cacheLatencies.append(endpoints_caches[e][c])
        idx_caches_ranked = np.argsort(cacheLatencies)
        endpoints[e]['caches'] = cacheIDs[idx_caches_ranked]
    print("\nEndpoints with caches ranked by their latencies")
    pprint(endpoints)

    ## CACHES (serving a list of endpoints)
    caches = []
    caches = [{'id':i,
               #'videos': [],
               'endpoints':[]} for i in xrange(nb_caches)]
    for e in endpoints:
        for cID in e['caches']:
            caches[cID]['endpoints'].append(e['id'])
    print("\nCaches (each element has its list of endpoints)")
    pprint(caches)

    ## CACHES: re-ordered so that the most connected caches appear first
    caches_reordered = []
    while len(caches)>0:
        mxEndpts = -1
        idMxC = -1
        for i,c in enumerate(caches):
            if len(c['endpoints']) > mxEndpts:
                idMxC = i
                mxEndpts = len(c['endpoints'])
        caches_reordered.append(caches[idMxC])
        del caches[idMxC]
    caches = np.copy(caches_reordered)
    print("\nCaches re-ordered")
    pprint(caches)
    mostConnected_caches = [c['id'] for c in caches]
    print "mostConnected_caches: ", mostConnected_caches
    
    ## REQUEST DESCRIPTIONS BY ENDPOINTS PER VIDEOS
    videos = []
    videos_endpoints = np.zeros((nb_videos, nb_endpoints), dtype=np.int)
    for r in xrange(nb_requestsDesc):
        infosR = lines.pop(0).split(" ")
        videoID = int(infosR[0])
        endpointID = int(infosR[1])
        nb_req = int(infosR[2])
        videos_endpoints[videoID][endpointID] = nb_req
    print("\nVideos x Endpoints:  requests matrix")
    print videos_endpoints

    videos_sumEndpoints = np.sum(videos_endpoints, axis=1)
    print("\nVideos requests summed over endpoints:  requests vector")
    print videos_sumEndpoints

    print('\nsort videos per "popularity" = nb. of requests over all endpoints')
    print("Indices of Videos ranked per total nb. of requests")
    print("=What are the videos that requires caching??")
    idx_videos_ranked = np.argsort(-videos_sumEndpoints)
    print idx_videos_ranked
    
    print("\nVideos requests ranked per total nb. of requests")
    videos_ranked = videos_sumEndpoints[idx_videos_ranked]
    print videos_ranked

    # First videos whose nb. request not null
    print("\nVideos ranked per total nb. of requests whose requests are not null")
    videos_ranked_not_null = videos_ranked[np.where(videos_ranked > 0)]
    print videos_ranked_not_null
    nb_videos_ranked_not_null = len(videos_ranked_not_null)
    print(nb_videos_ranked_not_null, "videos")

    # sorted videos x endpoints matrix
    print("\nVideos ranked per total nb. of requests (and not null) x endpoints")
    videos_endpoints_ranked = videos_endpoints[idx_videos_ranked]
    idx_videos_ranked_not_null = idx_videos_ranked[:nb_videos_ranked_not_null]
    videos_endpoints_ranked_not_null = videos_endpoints_ranked[:nb_videos_ranked_not_null]
    pprint(videos_endpoints_ranked_not_null)

    ## update endpoints with a list of videoIDs ranked by requests
    endpoints_videos = np.transpose(videos_endpoints_ranked[:nb_videos_ranked_not_null])
    for e in xrange(nb_endpoints):
        endpoints[e]['videos'] = []
        #print "rqs:", endpoints_videos[e]
        for iv,r in enumerate(endpoints_videos[e]):
            if r != 0:
                v = idx_videos_ranked[iv]
                #print r, iv ,v
                endpoints[e]['videos'].append(v)
    print("\nEndpoints now with videos ranked by nb.requests")
    pprint(endpoints)
    
    return (nb_videos, video_sizes, nb_endpoints, nb_requestsDesc, nb_caches, nb_MBperCache,
            endpoints, endpoints_caches, mostConnected_caches, videos_endpoints,
            nb_videos_ranked_not_null, idx_videos_ranked)


def get_score(videoIDs_in_caches,
              videos_endpoints, endpoints, endpoints_caches):
    """ in the form:
        [[videoID_10, videoID_22], # videos in cache #0
         [videoID_03, videoID_34], # videos in cache #1
         ...}
    """
    endpoints_videos = np.transpose(videos_endpoints)
    tot_time_saved = 0
    score = 0
    tot_req = 0
    for e, endp in enumerate(endpoints):
        cacheIDs = endpoints[e]["caches"]
        ld = endpoints[e]["Ld"]
        lc_arr = endpoints_caches[e]
        videosInDemand = copy.deepcopy(endpoints[e]['videos'])
        videosRequests = endpoints_videos[e]
        for v in videosInDemand:
            tot_req += videosRequests[v]
        ## Checking if any video added to the cache are used by the endpoint (only once by the fastest cache server)
        for c in cacheIDs:
            for v in videoIDs_in_caches[c]:
                if v in videosInDemand:
                    idx_v = videosInDemand.index(v)
                    #print "\nvideo #{v} in cache #{c} and used by endpoint #{e}".format(v=v, c=c, e=e)
                    #print "ld-lc=", ld-lc_arr[c]
                    time_saved = (ld-lc_arr[c]) * videosRequests[v]
                    tot_time_saved += time_saved
                    #print "time saved : {: 8d}".format(time_saved)
                    #print "total time_saved: {: 8d}".format(tot_time_saved)
                    #print "nb. requests: ", videosRequests[v]
                    del videosInDemand[idx_v]
                    #print "videos in demand", videosInDemand
                    break
    #print "tot_req.=", tot_req
    if tot_req == 0:
        score = 0
    else:
        score = int(np.floor(tot_time_saved * 1000. / tot_req))
    return score


def check_video_subset_fit_in_caches(videoIDs_in_caches, video_sizes, nb_MBperCache):
    mem_used = np.zeros(len(videoIDs_in_caches,), dtype=np.int)
    for cID, lst_v in enumerate(videoIDs_in_caches):
        for vID in lst_v:
            mem_used[cID] += video_sizes[vID]
            if mem_used[cID] > nb_MBperCache:
                return False
    #print "\nPercentage of memory used in each cache"
    #pprint(mem_used*100./nb_MBperCache)
    return True

def writing_videos_in_caches(videoIDs_in_caches, outFile="test.out"):
    #pprint(videoIDs_in_caches)
    nb_caches = len(videoIDs_in_caches)
    with open(outFile,'w') as o:
        o.write(str(nb_caches)+'\n')
        for c in videoIDs_in_caches:
            o.write(" ".join(str(i) for i in c) +'\n')

def cut_the_crap(scoresDelta, factor):
    if len(scoresDelta) < 2:
        return True
    elif scoresDelta[-1] > 1.0*factor*scoresDelta[1]:
        return True
    else:
        return False

def solve_with_common_sense(endpoints, videos_endpoints, idx_videos_ranked, nb_videos_ranked_not_null,
                            video_sizes,nb_MBperCache, nb_caches, endpoints_caches, mostConnected_caches,
                            outFile=outFile):
    """
    Base on the shape of the matrix videos_endpoints_ranked showing [videos,endpoints] ranked along video axis
    according to the highest total number of requests.
    Pseudocode:
    1. from a COPY of videos_endpoints_ranked, get stats on the distribution of requests,
       define a minimum size of requests to consider: smallestNbReq based on quantile
    2. consider dispatching videos only if #req is larger than the minimum
    3. select one video to add to one cache based on:
       * the most "popular" video which has highest #tot_reqs
       * select the fastest cache from the endpoints having most req for that video
    4. add the video to endpoints[e]['videosInMyCache']
    5. recompute the matrix "videos_endpoints_ranked_not_null" after annealing 
       push(v in e[fastest_cache]) IF-AND-ONLY-IF v not in e['videosAlreadyInOneOfMyCaches']
    """
    videoIDs_in_caches = []
    [videoIDs_in_caches.append([]) for c in xrange(nb_caches)]
        

    videos_endpoints_ranked_not_null = np.copy(videos_endpoints[idx_videos_ranked][:nb_videos_ranked_not_null])
    df = pd.DataFrame(videos_endpoints_ranked_not_null)
    df_req_notNull = df[df>0]
    mean = df_req_notNull.mean(axis=0,skipna=True).mean(axis=0,skipna=True)
    std = df_req_notNull.std(axis=0,skipna=True).mean(axis=0,skipna=True)
    ## 68% of requests are in mean +/- 1*std
    ## 95% of requests are in mean +/- 2*std
    ## 99% of requests are in mean +/- 3*std

    print "mean",mean
    print "std",std
    
    # ## Consider gradually more videos to dispatch based on the "popularity"

    # ### init
    # videoIDs_in_caches = []
    # isFittingCacheSize = True
    # quantile = 0.4
    # pprint(df_req_notNull.quantile(q=quantile, axis=1, numeric_only=True))
    # pprint(df_req_notNull.quantile(q=quantile, axis=0, numeric_only=True))
    # smallestNbReq = max(df_req_notNull.quantile(q=quantile, axis=1, numeric_only=True))
    # print "smallestNbReq: ",smallestNbReq

    # sys.exit()
    # for eID, e in enumerate(endpoints):
    #     e['vidAlreadyInOneOfMyCaches'] = []
    # while isFittingCacheSize:    
    #     isFittingCacheSize = False #check_video_subset_fit_in_caches(videoIDs_in_caches, video_sizes, nb_MBperCache)


    newScore = 0
    iter = -1
    while iter < nb_videos_ranked_not_null-1: # Best would be not estimate when a cache is nearly full, with no chance to accept one more video
        oldScore = -1
        iter += 1
        print "#"*100
        print "iter={iter}/{tot_iter}".format(iter=iter,tot_iter=nb_videos_ranked_not_null)
        top_video = videos_endpoints_ranked_not_null[iter]
        top_video_ID = idx_videos_ranked[iter]
        #print "Id for the current top video: ", top_video_ID
        # get endpoints where the video ranked #1 is present
        endpts_listeners_set = Set()
        potential_caches_set = Set()
        for eID,rqs in enumerate(top_video):
            if rqs > mean: ## CAREFUL, this parameter can be tuned to consider more/less endpoints wrt #requests. Try 0 or mean, or mean-std
                endpts_listeners_set.add(eID)
                [potential_caches_set.add(c) for c in endpoints[eID]['caches']]
        potential_caches = list(potential_caches_set)

        ## Re-order the potential caches
        potential_caches_reordered = []
        for pc in mostConnected_caches:
            if pc in potential_caches:
                potential_caches_reordered.append(pc)
        potential_caches = copy.deepcopy(potential_caches_reordered)
        
        if len(potential_caches) == 0:
            #print "Nothing to cache with this video"
            continue
        #print "endpts_listeners_set: ", endpts_listeners_set
        #print "potential_caches:", potential_caches
        # compute the score for various combination of videos in caches
        test = 0
        scoresDelta = []
        print cut_the_crap(scoresDelta, factor=0.1)
        while newScore > oldScore and cut_the_crap(scoresDelta, factor=0.1):
            scoresDelta.append(newScore-oldScore)
            print "newScore= ",newScore
            print "oldScore= ", oldScore
            print "newScore-oldScore= ",newScore-oldScore
            oldScore = newScore
            test += 1
            # keep adding the same video to more caches. Start with one cache, see if more caches improves
            # Build the possible configurations to test
            test_videoIDs_in_caches = [ copy.deepcopy(videoIDs_in_caches) for c in xrange(len(potential_caches))]
            #print "\nConfigurations before adding the top video to the caches"
            #pprint(test_videoIDs_in_caches)
            for t in xrange(len(potential_caches)):
                cacheID = potential_caches[t]
                #print "cacheID to add the video to: ", cacheID
                if top_video_ID not in test_videoIDs_in_caches[t][cacheID]:
                    temp_config = copy.deepcopy(test_videoIDs_in_caches[t])
                    test_videoIDs_in_caches[t][cacheID].append(top_video_ID)
                    isFittingCacheSize = check_video_subset_fit_in_caches(test_videoIDs_in_caches[t],
                                                                          video_sizes,
                                                                          nb_MBperCache)
                    if not(isFittingCacheSize):
                        #print "configuration exceeds memory of some cache. Revert to previous configuration!"
                        test_videoIDs_in_caches[t] = temp_config
            #print "New configurations to test"
            #pprint(test_videoIDs_in_caches)
            
            # compute the score for each new configuration
            test_scores = [get_score(confCache, videos_endpoints, endpoints, endpoints_caches) for confCache in test_videoIDs_in_caches]
            #print "scores:"
            #pprint(test_scores)
            idx_bestConf = np.argmax(test_scores)
            #print "idx_bestConf: ", idx_bestConf
            newScore = max(test_scores)
            if newScore > oldScore:
                videoIDs_in_caches = copy.deepcopy(test_videoIDs_in_caches[idx_bestConf])
                writing_videos_in_caches(videoIDs_in_caches, outFile=outFile)
            #pprint(videoIDs_in_caches)
    return videoIDs_in_caches

            

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.kdc.util;

import io.airlift.log.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * @author jake.zhang zhangxj@kingnet.com
 * @date 2019-11-28 16:52
 */
public class FunnelUtil
{
    private static final Logger log = Logger.get(FunnelUtil.class);

    private List<String> actions = new ArrayList<>();
    private List<Integer> timestamps = new ArrayList<>();
    private List<ArrayList<String>> funnelSteps;

    public FunnelUtil(Map<String, HashSet<Integer>> actionLists, List<ArrayList<String>> funnelSteps)
    {
        // change action list to actions list and timestamps list
        for (String step : actionLists.keySet()) {
            HashSet<Integer> stepTimes = actionLists.get(step);
            log.debug("now step : " + step + ", now step size is : " + stepTimes.size());
            for (Integer stepTime : stepTimes) {
                actions.add(step);
                timestamps.add(stepTime);
            }
        }
        this.funnelSteps = funnelSteps;
        log.debug("actions size : " + actions.size());
        log.debug("timestamps size : " + timestamps.size());
        log.debug("funnelsteps size :" + funnelSteps.size());
    }

    public List<Long> computer()
    {
        int inputSize = actions.size();
        int currentFunnelStep = 0;
        int funnelStepSize = funnelSteps.size();
        List<Long> ret = new ArrayList<>(Collections.nCopies(funnelStepSize, 0L));

        Integer[] sortedIndex = IntStream
                                        .rangeClosed(0, actions.size() - 1)
                                        .boxed()
                                        .sorted(this::funnelAggregateComparator)
                                        .toArray(Integer[]::new);

        for (int i = 0; i < inputSize && currentFunnelStep < funnelStepSize; i++) {
            if (funnelSteps.get(currentFunnelStep).contains(actions.get(sortedIndex[i]))) {
                ret.set(currentFunnelStep, 1L);
                currentFunnelStep++;
            }
        }

        // debug mode
        log.debug("debug mode :::: actions : " + ArrayUtil.ListStringOf(actions));
        log.debug("debug mode :::: timestamps : " + ArrayUtil.ListIntegerStringOf(timestamps));
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < sortedIndex.length; i++) {
            sb.append("|").append(sortedIndex[i]);
        }
        log.debug("debug mode :::: sortedIndex size : " + sortedIndex.length + ", info : " + sb.toString());
        log.debug("debug mode :::: ret : " + ArrayUtil.ListLongStringOf(ret));
        return ret;
    }

    private int funnelAggregateComparator(Integer i1, Integer i2)
    {
        int ret = ((Comparable) timestamps.get(i1)).compareTo(timestamps.get(i2));
        if (ret == 0) {
            return ((Comparable) actions.get(i1)).compareTo(actions.get(i2));
        }
        return ret;
    }
}

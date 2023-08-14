# Homework 2: Naive Bayes in Hadoop MapReduce

In our second assignment we'll dive deeper into the Map Reduce paradigm and introduce a new framework for parallel computation: Hadoop MapReduce. This assignment has historically been one of the most challenging for students because of the learning curve associated with this techonology and the programming paradigm. This semester we used the live sessions to give you a structured introduction to the framework and Hadoop syntax before you go on to use it to perform NaiveBayes. If you have not yet completed the `wk2Demo` and  `wk3Demo`notebooks you should do that now before the homework. We strongly encourage you to start early and to support each other on Slack.

### How to submit your work:

To complete the Homework 2 assignment you will need to submit your responses on the Canvas assignment form located [here](https://canvas.instructure.com/courses/4745907/assignments) for HW2.

We strongly recommend completing and saving all responses in the Homework  notebook first, before attempting to submit the assignment responses in Canvas, because:

All responses in Canvas must be submitted at once (i.e. on Canvas you can't save a half-completed assignment and come back later).
For full marks you will be required to upload an HTML and .ipynb version of your notebooks on the Canvas form, so it will need to be completed before submission.

Tips:
Make use of your peers and TAs by asking questions on Slack. Everyone comes to MIDs from a different background so don't be shy; all questions are welcome!


### Tips:

For HW2 and lab2, the [Total Order Sort notebook](https://github.com/UCB-w261/main/blob/main/HelpfulResources/TotalSortGuide/total-sort-guide-hadoop-streaming.ipynb ) is great. We recommendad the following workflow:
  * Section Hadoop Shuffle review
  *  Total order sort (https://github.com/UCB-w261/main/blob/main/HelpfulResources/TotalSortGuide/total-sort-guide-hadoop-streaming.ipynb)
  *  II.C.1. Hadoop Streaming Implementation - single reducer    (APP: top 10 most frequent words) Total order sort
  * II.C.2. Hadoop Streaming Implementation - multiple reducers (APP: top 10 most frequent words) Partial order sort
  *  II.C.3 Hadoop Streaming Implementation - multiple reducers with ordered partitions
  *  Section 3: Order inversion pattern with one reducer
 *  Section 5: Order inversion pattern with multiple reducers (optional)

In addition, and in a general,
* Do the readings! Many of the tasks and conceptual questions in this assignment are directly explained in the texts (se notes in the workbooks).
* If you are struggling with Hadoop, take a moment to read our [Debugging Tips](https://github.com/UCB-w261/main/blob/master/Resources/debugging.md).
* As always we ask that students don't share code because we want your submissions to accurately reflect your own coding style and understanding. However you will likely find it very tempting to share code for debugging purposes. *Help each other debug without sharing code*. Here's the right way to ask for help from your peers or TAs:
  1. **State exactly where in the HW you are working**.
  2. **List the precise error message you are getting** (this should be just a line or two... not the whole Hadoop mess!)
  3. **Describe where you've already looked for solutions** (this will help us avoid repeating things like "read the Debugging file" or "look in the Hadoop UI task logs")
  4. **Describe what you've already tried** (this will help us narrow down the issue and give you better suggestions)
  5. **Respond to other people's questions even if you aren't sure you know the solution!** (an _"I wonder if it could be..."_ or _"I saw something that could be similar which I solved by..."_ goes a long way in a class that has everone working at 150%).
  6. **Use Slack's thread feature** 

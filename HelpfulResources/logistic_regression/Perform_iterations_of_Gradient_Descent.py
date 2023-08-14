# This code performs the Gradient Descent iterations 
def GradientDescent_wReg(trainRDD, wInit, nSteps = 20, learningRate = 0.1,
                         regType = None, regParam = 0.1, verbose = False):
    """
    Perform nSteps iterations of regularized gradient descent and 
    track loss on a test and train set. Return lists of
    test/train loss and the models themselves.
    """
    # initialize lists to track model performance
    train_history, test_history, model_history = [], [], []
    
    # perform n updates & compute test and train loss after each update
    model = wInit
    for idx in range(nSteps):  
        # update the model
        model = GDUpdate_wReg(trainRDD, model, learningRate, regType, regParam)
        
        # keep track of test/train loss for plotting
        train_history.append(LogLoss(trainRDD, model))
        #test_history.append(LogLoss(testRDD, model))
        model_history.append(model)
        
        # console output if desired
        if verbose:
            print("----------")
            print(f"STEP: {idx+1}")
            print(f"training loss: {LogLoss(trainRDD, model)}")
            print(f"test loss: {LogLoss(testRDD, model)}")
            print(f"Model: {[round(w,3) for w in model]}")
    return train_history, test_history, model_history
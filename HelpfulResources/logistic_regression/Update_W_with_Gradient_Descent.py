# This code calculates the 3 different gradients with and without regularization
# Then it updates the model (w) and outputs the new model
def GDUpdate_wReg(dataRDD, W, learningRate = 0.1, regType = None, regParam = 0.1):
    """
    Perform one gradient descent step/update with ridge or lasso regularization.
    Args:
        dataRDD - tuple of (y, features_array)
        W       - (array) model coefficients with intercept at index 0
        learningRate - (float) defaults to 0.1
        regType - (str) 'ridge' or 'lasso', defaults to None
        regParam - (float) regularization term coefficient
    Returns:
        model   - (array) updated coefficients, intercept still at index 0
    """
    # augmented data
    augmentedData = dataRDD.map(lambda x: (x[0], np.append([1.0], x[1])))
    
    new_model = None

    def GradientPerRow(line):
        # Calculates -y(1- 1/ 1+ exp( -ywx))x for each row
        true_y, features = line
        predicted_y = np.dot(np.transpose(W),features)
        yield -1.0*true_y*(1.0 - 1.0/(1.0 + np.exp(-1.0*true_y*predicted_y)))*features
        
    # Use the same way as before to find the first component of the gradient function
    grad = augmentedData.flatMap(GradientPerRow).sum()
    
    # Take out the bias stored in index 0 of W
    model = W[1:]
    
    # Figure out the regulation component
    if regType == None:
        pass
        
    elif regType == 'lasso':
        reg_comp = regParam*np.sign(model)
        # Update the gradient function by taking the regularization component into consideration
        grad = grad + np.append(0,reg_comp)
                
    elif regType == 'ridge':
        reg_comp = regParam*model
        # Update the gradient function by taking the regularization component into consideration
        grad = grad + np.append(0,reg_comp)
    
    new_model = W - (learningRate*grad)
    
    return new_model
def LogLoss(dataRDD, W):
    """
    Compute the Log Loss of our model.
    Args:
        dataRDD - each record is a tuple of (y, features_array)
        W       - (array) model coefficients
    """
    # Add 1 to the front of the predictors array
    # Note that the b value has to be added to the front of our theta array
    augmentedData = dataRDD.map(lambda x: (x[0], np.append([1.0], x[1])))

    def LogLossPerRow(line):
        # Calculate log(1 + exp(-ywx)) for each row of the data in parallel
        actual_y, features = line
        predicted_y = np.dot(np.transpose(W),features)
        yield np.log(1.0 + np.exp(-1.0*actual_y*predicted_y))
    
    loss = augmentedData.flatMap(LogLossPerRow).sum()

    return loss
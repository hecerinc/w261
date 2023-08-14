# Some handy commands

## Github
github doesn't allow files lager than 100MB to be uploaded. Once you commit large files, it is tricky to remove them from history and github and you will not be able to push subsequent commits. To check if there are any files larger than 100MB in your repo's directory you can run the following commands:   

To find files larger than 100MB:   
`find . -type f -size +100M`   

If you want the current dir only:   
`find . -maxdepth 1 -type f -size +100M`   
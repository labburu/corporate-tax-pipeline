import re

def FileRename(filename):
  # strip off file extension
  format_filename = filename.rsplit( ".", 1 )[ 0 ]  
  # Remove all non-word characters (everything except letters)
  regex = re.compile('[^a-zA-Z]')
  format_filename = regex.sub('', format_filename)
  format_filename = format_filename.replace('response', '')
  # print(format_filename)

  return format_filename



def ExtractUserName(dataFolderPath):
    pathSegments = dataFolderPath.split("/")
    userString = pathSegments[-2]
    userformatted = re.sub(r'_(\d+)|(\d+)$','_',userString)
    return userformatted.strip("_")
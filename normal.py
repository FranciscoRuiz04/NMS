######################  Generators  ######################


def recFun(generator):
    s_words = []
    filteredRec = []
    for field in generator:
        value = field.strip()
        if value.isalpha():
            s_words.append(value)
        else:
            filteredRec.append(value)
    s_words = '_'.join(s_words)
    #Completed words as members of the list
    if len(s_words) != 0:
        filteredRec.insert(0, s_words)
    yield tuple(filteredRec)


#--------------------------------------------------------

######################  Functions  ######################


def sorter(filepath, del_lines=0, headers=False, ulmt=0, sep=None):
    """
    Separate header from table data from a text filepath.\n
    Parameters.\n
    <filepath>
    Path filepath.
    
    <del_lines>
    Number of lines to be deleted. By default is 0
    
    <headers>
    File has or not headers. By default is FALSE.
    
    <ulmt>
    Use solely lines with a number of elements major than...
    By default is 0.
    """

    #Load filepath and get every text line
    try:
        file = open(filepath, encoding='utf-8')
        readerLines = (line.split(sep) for n, line in enumerate(file)
                       if n >= del_lines)

    except FileExistsError as err:
        print(err)

    else:
        content = {}  #Separated element dataframe container
        data = []  #Only data container

        #Actions by record
        for i, record in enumerate(readerLines):
            #Solely tables
            if len(record) > ulmt:
                if i == 0 and headers:
                    content["HEADERS"] = record
                else:
                    cleanRecord = recFun(record)
                    for rec in cleanRecord:
                        data.append(rec)
        content["DATA"] = data
        return content
    finally:
        file.close()


#--------------------------------------------------------


class ExtData:
    """
    Separate records from a text-file.
    
    <filepath>
    Filepath
    
    <del_lines>
    Number of lines to be deleted. By default is 0
    
    <headers>
    File has or not headers. By default is FALSE.
    
    <ulmt>
    Use solely lines with a number of elements major than...
    By default is 0.
    
    <colnames>
    Assing number or string data type as column names to the dataframe.
    If headers is TRUE this parameter will be deprecated.
    """

    def __init__(self,
                 filepath,
                 del_lines=0,
                 headers=False,
                 ulmt=0,
                 colnames=0,
                 sep=None):
        tab = sorter(filepath, del_lines, headers, ulmt, sep)

        #Assing column names
        if headers: self.headers = tab["HEADERS"]
        else: self.headers = colnames

        self.data = tab["DATA"]

    def __str__(self):
        return ("\nCOLNAMES:\n%s\n\nDATA:\n%s\n" % (self.headers, self.data))


if __name__ == '__main__':
    import pandas as pd
    mydata = ExtData(r".\stats\SMN_1151.html", 4, 1, 4)
    df = pd.DataFrame(mydata.data, columns=mydata.headers)
    print(df)
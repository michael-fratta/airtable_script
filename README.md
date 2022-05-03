
A Python script - running automatically, on a (hardcoded) scheduler; bundled as an app and hosted on the cloud platform Heroku - that, essentially, updates one database (Airtable) with data from another (Fauna).  The steps it follows are explained - concisely - below (see code for full detail):

• imports a given Airtable table into a variable, using the Airtable API

• creates a list from the values contained within a given column from the Airtable table we just imported

• imports a given Fauna document collection into a variable, using the Fauna API

• creates a list from the values contained within a given object from the Fauna document collection we just imported

• compares these two lists we just created - and creates another list that contains only the values from Fauna which are not already present in Airtable

• imports a document from an SFTP server, using the pysftp library, that contains a list of values that can be ignored

• iterates through the list of values not already present in Airtable, and removes those values present in the 'ignore list' - creating another list

• iterates through this list we just created, finds the corresponding data from Fauna, and appends new rows - containing the data from Fauna we just fetched - into the Airtable table we referenced earlier

• creates another list of rows that were just created

• iterates through this list, and queries the Lloyd Latchford (car insurance) API - to determine if the given car (that we just added to Airtable) has insurance

• attaches the result to a list, if it does not

• updates all the values in a given column in Airtable, with the corresponding value from Fauna - if that value has changed (in Fauna) since the last run of the script

• posts relevant updates/actions to a Slack (messaging service) channel, as a message, via the Slack API

I am the sole author of this script. Revealing keys/values/variables/file names have been replaced with arbitrary/generic ones - for demonstrative purposes only.

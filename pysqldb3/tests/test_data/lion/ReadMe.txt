SUPPLEMENTAL INFORMATION REGARDING THE BYTES OF THE BIG APPLE LION FILE

As of release 12C, the Department of City Planning will no longer issue a Locator files.  All the information formerly contained in that document has been incorporated into the metadata with the exception of the following:

1.  Layer Files (.lyr)

The complete LION geodatabase contains a variety of different types of segments, some of which a user may not want to view.  To help facilitate viewing the data, the following layer files are included for your convenience:

A. LION Streets - Generic.lyr:  Displays all generic street features

B. LION Streets - Roadbeds.lyr:  Displays all multi-roadbed street features

C. LION - Generic.lyr:  Displays all non-street features along with generic street features

D. LION - Roadbeds.lyr:  Displays all non-street features along with multi-roadbed street features

E. LION - Street Name Labels.lyr: Displays street name annotation for roadbed geography

F. LION- Street Direction Arrows.lyr (REQUIRES MAPLEX):  

For ArcGIS 9.3

Displays traffic directions for roadbeds. This feature requires that the Maplex extension is installed and enabled and the Maplex Label Engine is turned on.  To enable this extension go to:

	1) Tools -> Extensions
	2) Check Maplex extension
	3) Tools -> Customize
	4) Check the labeling toolbar
	5) Once the toolbar is enabled, click on the "labeling" dropdown arrow
	6) Click "Use Maplex label engine"
	7) Click the label manager on the labeling toolbar
	8) Highlight the default label class under "LION - Street Direction Arrows"
	9) Click properties
	10) Under Label position click "orientation"
	11) Check the label alignment box to set the labels to direction of line
	12) Click Ok
	13) Click Apply
    


ArcGIS 10 - using Cartographic Representation

	1) Copy TrafficRepArrows.style to the Style folder in the ArcGIS installation folder.
	2) Open LION feature layer in ArcMap
	3) Right Click LION feature layer and click "Propierties" and symbolize layer by "Unique values" under categories.
	4) From the dropdown box under the Value field select "TrafDir". Click "Add all values" and uncheck the all other values option and click ok.
	3) Right Click LION feature layer and click on "Convert Symbology to Representation" to create a new representation called "lion_TD"
	4) By default a new Representation layer is added to the ArcMap document
	5) Right click the "lion_TD" Representation layer -->  go to layer properties -->Symbology and click Representations
	6) Click "[1]" under representation and change the line color to null
	7) Click "[2]A", change line color to null and add a New Marker layer (plus with a pin sign). A new Marker layer is added (pin button).
	8) Click on the Marker symbol, this will open up the Representation Marker Selector. 
	9) click More Styles and check the TrafficRepArrows style.
	10) Select the "Rep Line Decoration 1", Click properties to take you to the Marker Editor to change the color. Select the arrow and change the 
	    color under color Pattern and click ok.
	11) Change size to 15, Angle to 180
	12) Change the Marker placement by Clicking the "Black Arrow Button" on the Marker window and select "On Line" and leave the default values.
	13) Click "[3]P" under representation and change the line color to null.
	14) Click "[4]T" under representation and change the line color to null and follow the steps 7 to 12. 
	15) Select the "Rep Line Decoration dual 2" and set the following properties. Size to 16 and Angle to 0
	16) Click "[5]W" under representation and change the line color to null and follow the steps 7 to 12. Set the Marker Angle to 0.
	

2.  Understanding Street Names

Some streets may have multiple street names, some of which are valid for the full length of the street (for example, "6 Avenue" and "Avenue of the Americas" in Manhattan) while others apply to only a portion of the full street.  For instance, a portion of "West 110 Street" in Manhattan has the alternative valid street name "Central Park North'; a different portion of "West 110 Street" has the alternative street name "Cathedral Parkway".  The addresses "155 West 110 Street" and "460 Cathedral Parkway" are equivalent; the addresses "460 West 110 Street" and "460 Cathedral Parkway" are also equivalent.  However, "155 Cathedral Parkway" and "460 Central Park North" are not valid addresses, since those street names are not valid for the portions of "West 110 Street" where those respective address numbers are located.

Additionally, there may be multiple valid spellings for a particular street name.  For example, it is acceptable to refer to Adam Clayton Powell Boulevard in Manhattan as Powell Boulevard, Adam Powell Boulevard or A C Powell Boulevard.

Finally, there are some common misspellings of certain street names.  For example, in Brooklyn, Reed St, Richards St and Sandford St are sometimes misspelled Reid St, Richard St, and Sanford St, respectively.

The LION files account for most of these cases through the use of Street Codes and the LGC fields which establish what names are valid for a particular segment.


3.  Join_ID

This is an identification field used to link LION features to the Alternate Names table during a geocoding operation.

Join_ID is the concatenation of Boro/FaceCode/LGC1/LGC2/LGC3/LGC4 and for SAF records, it is Boro/StreetCode/LGC1/LGC2/LGC3/LGC4/SpecAddr.  The alternate street name table is built using all the street names that correspond to the street code and lgc values that are in the Join_ID.  This can be a many-to-many relationship.

A simple example, Absecon Road in Manhattan, has a Join_ID of 1050001000000 .  There is only one LION record (1 segment) with this Join_ID, and the only LGC associated in this Join_ID is LGC 01.  In the altnames table, there is 1 corresponding record with the same Join_ID for Absecon Rd since that is the only valid name associated with that LION segment.

A more complex example is Adam C Powell Blvd.  There are well over 100 LION segments with this name, and 10 different Join_IDs associated with the segments:

1051501040000
1051501040500
1051501040507
1051501040608
1051501040709
11674009000000X
11674010000000X  
11674011000000X
12246007000000X
12246008000000X
 
The first Join_ID for Adam C Powell Blvd is associated with 57 LION segments and has 11 corresponding records in the Altname table.  This represents the street name variants that are valid for LCGs 01 and 04 as indicated in the Join_ID:
 
A C P BLVD
ADAM C POWELL BLVD
A C POWELL BLVD
AC POWELL BLVD
ACP BLVD
ADAM CLAYTON POWELL BLVD
ADAM CLAYTON POWELL JR BLVD
ADAM POWELL BLVD
ADAM POWELL JR BLVD
POWELL BLVD
7 AVE
 
Each of the remaining Join_IDs also have numerous Altname records corresponding to the SND LGC entries.


4. Joint Interest Areas (JIAs)

Joint Interest Areas (JIAs) are major parks and airports that are not contained within any CD.  The JIA is returned in the The Community District fields (L_CD and R_CD) each of which contains a 3 byte code.  The first byte is the Borough Code, and the second and third bytes are the Community District Number (Right Justified, Zero Filled), or JIA. 
There are 59 community districts in the City of New York, as well as 12 Joint Interest Areas (JIAs) as follows:


164 	Central Park 
226 	Van Cortlandt Park 
227 	Bronx Park 
228	Pelham Bay Park 
355	Prospect Park 
356	Brooklyn Gateway National Recreational Area
480	LaGuardia Airport 
481	Flushing Meadows – Corona Park
482	Forest Park 
483	JFK International Airport 
484	Queens Gateway National Recreation Area
595	Staten Island Gateway National Recreation Area



import pandas as pd
import pycountry
import re
import phonenumbers
import networkx as nx

from functools import lru_cache

from geopy.geocoders import Nominatim

##//-----------------------------a. Country Recognition------------------------------

geolocator = Nominatim(user_agent="Sam_App")

cache_geocode = {}

def Geocode_Cache(Country_City):
    """
    Function that speeds up data search with geolocation by creating a cache of previously searched values.

    Args:
        country (str): The name of the country or city to search with geocode.

    Returns:
        geopy.Location or None: A geopy Location object containing the geocoded data for the specified country or city if the operation is successful.
        Returns None if the geocoding fails or if the country cannot be geocoded.
    
    Description:
        Function that checks if the geolocation data for the specified country or city is already queried. 
        If found, it return the result from the cache. Otherwise, search with geolocator and stores the value in the cache, 
        and returns the data. If an exception happens during geocoding, 
        it stores `None` in the cache for the given country and returns `None`.

    """
    if Country_City in cache_geocode:
        return cache_geocode[Country_City]
    try:
        result = geolocator.geocode(Country_City)
        cache_geocode[Country_City] = result
        return result
    except Exception as e:
        cache_geocode[Country_City] = None
        return None

def Clean_Country(Geopy_Location):

    """
    Cleans the information of the country name from a string.

    Args:
        Geopy_Location (str): A string with information of the country or city, potentially separated by '/ ' or '/'.

    Returns:
        str or None: The extracted name as a string clean , or None if no country can be determined.

    Description:
        This function checks if the input string contains a separator ('/ ' or '/'). If so, it splits the string at the separator
        and returns the second part as the country name. If no separator is present, it returns the original string. 
        If the string contains only one part without a separator, the country name is returned as is. If no valid country is found,
        it returns None.
    """
    
    if '/ ' in Geopy_Location:
        Countr = Geopy_Location.split('/ ')  
        Countr = Countr[1] if len(Countr) > 1 else None

    elif '/' in Geopy_Location:
        Countr = Geopy_Location.split('/') 
        Countr = Countr[1] if len(Countr) > 1 else None
    else:
        Countr = Geopy_Location
    return Countr

def Country_Recognition(Country_City):
    
    """
    Recognizes and extracts the country and city from a geolocation result.

    Args:
        Country_City (str): A string representing a country or city to be geocoded.

    Returns:
        tuple: A tuple containing two values:
            - The first value is the city name (str) if available, or None if not found.
            - The second value is the country name (str), or None if no valid country can be determined.

    Description:
        Function that first tries to find geolocation data for the given country or city. If successful, it splits the 
        geolocation result's display name into components. If only one component is found, it returns None for the city 
        and the country name. If multiple components are found, it returns the first as the city and the last as the country.
        The `Clean_Country` function is used to ensure that only the relevant country name is returned. If geolocation data 
        cannot be found, the function returns `(None, None)`.
    """

    VarCity = Geocode_Cache(Country_City)
    
    if VarCity is None:
        return (None, None)

    Tupla_Result = [x.strip() for x in VarCity.raw.get('display_name').split(',')]

    if len(Tupla_Result) == 1:
        Tupla_Result[0] = Clean_Country(Tupla_Result[0])
        return (None,Tupla_Result[0])
    else:
        Tupla_Result[-1] = Clean_Country(Tupla_Result[-1])
        return (Tupla_Result[0], Tupla_Result[-1])
    return None

##//-----------------------------End Country Recognition------------------------------

##//-----------------------------b. Found Emails------------------------------

def Found_Emails(raw_email):

    """
    Extracts the first valid email address from a given string.

    Args:
        raw_email (str): A string that may contain an email address.

    Returns:
        str or None: The extracted email address if found, or None if no valid email address is found.

    Description:
        This function uses a regular expression to search for a valid email address in the string. If a valid email 
        address is found, it returns the first match. If no valid email is found, it returns None.
    """

    correo = re.search(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', raw_email)
    return correo.group(0) if correo else None

##//-----------------------------End Found Emails------------------------------

##//-----------------------------c. Fix Phone Numbers ------------------------------

def Code_Phone(City_Country):

    """
    Returns the international dialing code for a given country.

    Args:
        City_Country (tuple): A tuple where the second element (`City_Country[1]`) is the full name of the country.

    Returns:
        str: The international dialing code in the format "(+X)" if the country is found.
        Returns "Country not found" if the country name is not recognized.
        Returns an error message as a string if an exception occurs.

    Description:
        This function uses the `pycountry` library to get the ISO 3166-1 alpha-2 country code for the given country name. 
        after uses the `phonenumbers` library to find the corresponding international dialing code.
        If the country is not found, it returns "Country not found". If an error occurs, it returns the exception message.
    """

    try:
        # Obtener el código de país de 2 letras usando pycountry
        Country = pycountry.countries.get(name=City_Country[1])
        if Country:
            Code_Country = Country.alpha_2  # El código de dos letras
            Code_Phone = phonenumbers.country_code_for_region(Code_Country)
            #return f"El código telefónico para {City_Country[1]} es: +{Code_Phone}"
            Code_Phone = f"(+{Code_Phone})"
            return Code_Phone
        else:
            return "Country not found"
    except Exception as e:
        return str(e)

def Clean_Phone(Phone):

    """
    Cleans and organizes a phone number by removing unnecessary characters and spaces.

    Args:
        Phone (str or None): The phone number to be cleaned. If None, the function returns None.

    Returns:
        str or None: The cleaned and formatted phone number as a string, or None if the input is None.

    Description:
        This function removes any caharacter:('-') and zeros('0') at the beginning from the phone number. 
        after organizes the number by inserting a space every four digits, starting from the right.
        If the input is None, it returns None without processing.
    """

    if Phone is None:
        return Phone
    else:
        if '-' in Phone:
            CleanPhone = Phone.replace('-', '')
            CleanPhone = CleanPhone.lstrip('0')
        else:
            CleanPhone = Phone
            CleanPhone = CleanPhone.lstrip('0')
        CleanPhone = re.sub(r'(?=(\d{4})+$)', ' ', CleanPhone)
        return CleanPhone

def Fix_Phone_Numbers (City_Country,Phone):

    """
    Combines country dialing codes with cleaned phone numbers to produce properly formatted (+XX) XXXX XXXXXX.

    Args:
        City_Country (Column.dataframe): Column from dataframe containing a (tuple) with country names.
        Phone (Column.dataframe): Column from dataframe containing raw phone numbers.

    Returns:
        pd.Series: A pandas Series with formatted phone numbers in the properly formatted".

    Description:
        This function applies the `Code_Phone` function to find the international dialing code for each country in 
        `City_Country` and the `Clean_Phone` function to clean each phone number in `Phone`. after this concatenates the 
        results, providing properly formatted phone numbers.
    """

    Phone_Numb = City_Country.apply(Code_Phone)+' '+Phone.apply(Clean_Phone)

    return Phone_Numb

##//----------------------------- End Fix phone Number-----------------------------



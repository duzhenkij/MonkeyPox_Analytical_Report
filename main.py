import pandas as pd
import datetime as dt

df = pd.read_csv("https://raw.githubusercontent.com/globaldothealth/monkeypox/main/latest.csv", low_memory=False)

# converting date of confirmation to datetime format
df['Date_confirmation'] = pd.to_datetime(df['Date_confirmation'], errors='coerce', dayfirst=True)
df['Date_death'] = pd.to_datetime(df['Date_death'], errors='coerce', dayfirst=True)

# the dataframe has a little trouble in Gender's series.
# There are not only 'female' and 'male' but also 'male ' with whitespace at the end. It has to be corrected
df['Gender'] = df.Gender.str.strip().str.lower()

# rename Great Britain
great_britain_parts = [r'^England$', r'^Scotland$', r'^Wales$', r'Northern Ireland$', r'^Gibraltar$']
df = df.replace(regex=great_britain_parts, value='Great Britain')

# confirmed cases selection
df = df[df['Status'] == 'confirmed']

# filling in empty cells in the table
df.Gender = df.Gender.fillna('None')

'''Population distribution by gender'''
confirmed_cases_by_gender = df.groupby('Gender')['ID'].\
    count().to_frame().\
    sort_values(by='ID', ascending=False).\
    rename(columns={'ID': 'ID_count'})


'''Population distribution by age categories'''
# there is a dictionary where keys are the common age categories and values of the age categories from dataframe
# this is a rough attempt to generalize the age categories which are presented in different ways
common_gender_categories_dict = {
    'till_18': ['15-19', '10-14', '5-9', '0-5'],
    'from_18_till_60': ['20-44', '30-59', '40-44', '25-29', '20-59',
                        '30-34', '20-64', '40-49', '30-39', '15-74',
                        '35-39', '50-59', '<40', '55-59', '45-49',
                        '20-24', '30-50', '50-54', '15-64', '25-49',
                        '40-45', '26-54', '22-55', '30-54', '30-49',
                        '20-29', '40-42', '15-69', '20-50', '45-50',
                        '35-40', '50-55', '20-69', '34-46', '20-62'],
    'from_60': ['70-74', '60-64', '65-69']
}

age_category = []
for i in list(df['Age']):
    if i in common_gender_categories_dict.get('till_18'):
        age_category.append('till_18')
    elif i in common_gender_categories_dict.get('from_18_till_60'):
        age_category.append('from_18_till_60')
    elif i in common_gender_categories_dict.get('from_60'):
        age_category.append('from_60')
    else:
        age_category.append('None')

df['age_category'] = age_category
confirmed_cases_by_age_category = df.groupby('age_category')['ID'].\
    count().\
    to_frame().\
    sort_values(by='ID', ascending=False).\
    rename(columns={'ID': 'ID_count'})

'''Population distribution by age categories, gender'''
confirmed_cases_by_gender_and_age_category = df.\
    pivot_table(values='ID', index='Gender', columns='age_category', aggfunc='count', fill_value=0)

'''Methods of confirmation'''
confirmation_methods = df.groupby(['Confirmation_method'])['ID'].\
    count().to_frame().\
    sort_values(by='ID', ascending=False).\
    rename(columns={'ID': 'ID_count'})

'''Frequency of different symptoms among people'''
# normalizing series
df['symptoms_normalized'] = df['Symptoms'].\
    str.lower().\
    str.replace(';', ',').\
    str.replace('headaches', 'headache').\
    str.replace('itching', 'itch').str.replace('muscle aches', 'muscle pain').str.replace('rashes', 'rash').\
    str.replace('vasicular', 'vesicular').\
    str.replace('outbreak on the skin, hands, and chest', 'outbreak on the skin and hands and chest').\
    str.strip()

# all symptoms (including comma-separated) from series to a list
symptoms = list(df['symptoms_normalized'])
separeted_symptoms = []

for i in symptoms:
    if type(i) == str:
        separeted_symptoms.extend(i.split(', '))

# all symptoms to dataframe
sym_df = pd.DataFrame({
    'symptoms': separeted_symptoms
})

# getting only unique symptoms from all using set
separeted_unique_symptoms = set(separeted_symptoms)

# searching for frequency of unique symptoms among all symptoms
symptom = []
freq = []
for i in separeted_unique_symptoms:
    symptom.append(i)
    freq.append(len(sym_df[sym_df['symptoms'] == i]))

# getting dataframe of symptoms' frequency
sym_freq = pd.DataFrame({
    'symptom': symptom,
    'freq': freq
})

symptomatic = sym_freq.sort_values(by='freq', ascending=False)

'''Dynamics of confirmed cases of monkeypox in time'''
dynamic_by_days = df.groupby(by=pd.Grouper(key='Date_confirmation', freq='D'))['ID'].\
    count().to_frame().\
    rename(columns={'ID': 'ID_count'}).\
    sort_index(ascending=False)

'''Dynamics of confirmed cases of monkeypox in countries'''
dynamic_by_countries = df.groupby(by=['Country'])['ID'].\
    count().to_frame().\
    sort_values(by='ID', ascending=False).\
    rename(columns={'ID': 'ID_count'})

'''Dynamics of death cases of monkeypox by dates'''
death_by_days = df.groupby(by=pd.Grouper(key='Date_death', freq='D'))['ID'].\
    count().\
    to_frame().\
    rename(columns={'ID': 'ID_count'}).\
    sort_index(ascending=False)

'''Dynamics of death cases of monkeypox by countries'''
dead_people = df[df['Date_death'].notna()]
death_by_countries = dead_people.groupby(by=['Country'])['ID'].\
    count().to_frame().\
    sort_values(by='ID', ascending=False).\
    rename(columns={'ID': 'ID_count'})

'''Dynamics of confirmed cases of monkeypox in countries by dates (count on date and cumulative count on date)'''
countries = sorted(list(df['Country'].unique()))
country_count_sets = []
for i in countries:
    count_by_countries_on_date_list = df[df['Country'] == i].\
        groupby(by=pd.Grouper(key='Date_confirmation', freq='D'))['ID'].\
        count().to_frame().\
        rename(columns={'ID': i})
    country_count_sets.append(count_by_countries_on_date_list)
df_countries_counts_by_day = pd.concat(country_count_sets, axis=1).fillna(0)
df_countries_counts_by_day_cumulative = df_countries_counts_by_day.cumsum()
country_confirmed_count_by_date = pd.\
    merge(df_countries_counts_by_day_cumulative,
          df_countries_counts_by_day,
          left_index=True,
          right_index=True,
          suffixes=('_cumulative', '_on_day'))
countries_confirmed_count_by_date = country_confirmed_count_by_date.\
    reindex(sorted(country_confirmed_count_by_date.columns), axis=1).\
    sort_index(ascending=False)

'''Output results to Excel file'''
date_now = str(dt.datetime.now()).split()[0]
with pd.ExcelWriter(fr'MonkeypoxReportOfGlobalHealth_{date_now}.xlsx') as writer:
    dynamic_by_days.to_excel(writer, sheet_name='DaysCases')
    dynamic_by_countries.to_excel(writer, sheet_name='CountriesCases')
    countries_confirmed_count_by_date.to_excel(writer, sheet_name='CountriesOnDays')
    confirmed_cases_by_gender.to_excel(writer, sheet_name='Gender')
    confirmed_cases_by_age_category.to_excel(writer, sheet_name='Age')
    confirmed_cases_by_gender_and_age_category.to_excel(writer, sheet_name='GenderAge')
    symptomatic.to_excel(writer, sheet_name='Symptoms', index=False)
    confirmation_methods.to_excel(writer, sheet_name='Methodics')
    death_by_days.to_excel(writer, sheet_name='DeathDays')
    death_by_countries.to_excel(writer, sheet_name='DeathCountries')

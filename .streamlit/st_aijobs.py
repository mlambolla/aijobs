import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
from collections import Counter
import plotly.express as px

#### streamlit config
st.set_page_config(
page_title="AI Jobs",
layout="wide"
 )
####


@st.cache()
def load_data():
    temp = pd.read_csv('https://raw.githubusercontent.com/mlambolla/aijobs/main/.streamlit/data/data_jobs.csv',sep='$')
    temp['skills'] = temp['skills'].map(lambda x: x.replace("'","").strip('][').split(', '))
    return temp

df = load_data()

def get_skills_from_profile(profile):
    skills_list = []
    data = df[df['title_normalized']==profile]
    for x in data['skills']:
        for item in x:
            skills_list.append(item)    
    types_counts=Counter(skills_list)
    df_skills = pd.DataFrame.from_dict(types_counts, orient='index').reset_index()\
        .rename(columns={'index':'skill',0:'count'}).sort_values(by=['count'],ascending=False).reset_index()  
    
    df_skills['skill'] = df_skills['skill'].map(str) + ' (' + df_skills['count'].map(str) + ')'
    return df_skills[['skill']]

#settings
color_scale = alt.Scale(domain=['Data Scientist','Data Engineer','Machine Learning Engineer','Data Analyst','Others'],range=['#626FFA', '#EF543A','#00CD97','#AA62FA','#FEA15B'])

st.markdown('<div style="color: blue;text-align:right;">Last update: ' + df['creation_date'].max() +'</div>',unsafe_allow_html=True)
st.markdown('<h2 style="color: black;text-align:center;">Demo - data extracted from some ai-jobs site since ' + df['creation_date'].min() +'</h2>',unsafe_allow_html=True)
st.markdown('<h3 style="color: black;text-align:center;">Technology used: <i>Python, Apache Airflow (daily schedule), AWS (S3, Redshift), Selenium, Altair, Plotly, Streamlit</h3>',unsafe_allow_html=True)


title_expander = st.beta_expander("Profiles Treemap", expanded=True)
with title_expander:
    df_title = df.groupby(['title_normalized'])['link'].count().rename('count').reset_index()
    cols_profile_container = st.beta_container()
    col1, col2 = st.beta_columns([2,1])
    with cols_profile_container:
        with col1:
            fig = px.treemap(df_title, path=['title_normalized'],values='count')
            st.plotly_chart(fig)
        with col2:
            st.text('')
            st.text('Profiles Table')
            df_title_porc = pd.concat([df_title,(round(df_title['count']/df_title['count'].sum(),3)*100).rename('porc')],axis=1)
            st.table(df_title_porc.sort_values('count',ascending=False).assign(hack='').set_index('hack'))


daily_charts_expander = st.beta_expander("Daily Activity Chart by Profiles",expanded=True)
with daily_charts_expander:
    for title in df['title_normalized'].unique():
        df_temp = df[df['title_normalized']==title]
        additions = df_temp.groupby(['title_normalized','creation_date'])['link'].count().rename('additions')\
            .reset_index().rename(columns={'creation_date':'date'}).set_index('date')

        deletions = df_temp.groupby(['title_normalized','delete_date'])['link'].count().rename('deletions')\
            .reset_index().rename(columns={'delete_date':'date'}).set_index('date')
        deletions['deletions'] = deletions['deletions']*-1
        t = pd.concat([additions['additions'],deletions['deletions']],axis=1).fillna(0).reset_index()\
            .melt(id_vars=['date'], var_name='type',
                value_name='count')
        t = t[t['date']> t['date'].min()]
        #display(t)
        daily_chart =alt.Chart(t, title=title).mark_bar(size=20).encode(
        x=alt.X('date',title=None),
        y=alt.Y('count',title=None),
        color=alt.Color('type',title=None),
        tooltip=['date','type','count']
        ).properties(height=300,width=600)
        st.altair_chart(daily_chart)
    
skills_expander = st.beta_expander("Skills by Profile", expanded=True)
with skills_expander:
    cols_profile_container = st.beta_container()
    col1, col2, col3, col4, col5 = st.beta_columns([1,1,1,1,1])
    with cols_profile_container:
        with col1:
            profile ='Data Analyst'
            st.text(profile)
            skills_df = get_skills_from_profile(profile)
            st.dataframe(skills_df.style.set_properties(**{'text-align': 'left'}))
        with col2:
            profile = 'Data Engineer'
            st.text(profile)
            skills_df = get_skills_from_profile(profile)
            st.dataframe(skills_df.style.set_properties(**{'text-align': 'left'}))
        with col3:
            profile = 'Data Scientist'
            st.text(profile)
            skills_df = get_skills_from_profile(profile)
            st.dataframe(skills_df.style.set_properties(**{'text-align': 'left'}))
        with col4:
            profile = 'Machine Learning Engineer'
            st.text(profile)
            skills_df = get_skills_from_profile(profile)
            st.dataframe(skills_df.style.set_properties(**{'text-align': 'left'})) 
        with col5:
            profile = 'Others'
            st.text(profile)
            skills_df = get_skills_from_profile(profile)
            st.dataframe(skills_df.style.set_properties(**{'text-align': 'left'}))


profiles_expander = st.beta_expander("Profiles by Company", expanded=True)
with profiles_expander:
    a_comp = df.groupby(['company'])['link'].count().rename('count').reset_index().sort_values('count',ascending=False)

    cols_company_profile_container = st.beta_container()
    col1, col2 = st.beta_columns([1,1])
    with cols_company_profile_container:
        with col1:
            porc = st.slider('Percentage of companies to visualize (highest above)',1,100,10)

    a_comp = a_comp.head(round(a_comp.shape[0]*(porc/100)))

    a =df[df['company'].isin(a_comp['company'])].groupby(['company','title_normalized'])['link']\
        .count().rename('count').reset_index()
    chart_top_companies = alt.Chart(a).mark_bar().encode(
        x=alt.X('count',title=None),
        y=alt.Y('company',title=None, sort='-x'),
        color=alt.Color('title_normalized',title=None,scale=color_scale),
        tooltip=['company','title_normalized','count']
    ).properties(width=800)

    st.altair_chart(chart_top_companies)




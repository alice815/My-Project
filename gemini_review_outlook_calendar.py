import win32com.client, datetime
from datetime import date
from dateutil.parser import *
import calendar
import pandas as pd
import re
import json
import os
from dotenv import load_dotenv
import google.generativeai as genai

#Get calendar events
Outlook = win32com.client.Dispatch('Outlook.Application')
ns = Outlook.GetNamespace('MAPI')

calendar = ns.GetDefaultFolder(9).Items

calendar.Sort('[Start]')
calendar.IncludeRecurrences = 'True'

#Set date range for calendar events
end = date.today() + datetime.timedelta(days=7)
end = end.strftime('%m/%d/%Y')
begin = date.today() - datetime.timedelta(days=7)
begin = begin.strftime('%m/%d/%Y')
calendar = calendar.Restrict("[Start] >= '" +begin+ "' AND [END] <= '" +end+ "'")

#Remove specail character, url, and Team online meeting info
def clean_body(body):
    # Remove URLs
    body = re.sub(r'http\S+|www\S+|https\S+', '', body, flags=re.MULTILINE)    
    # Remove backslashes and the first letter after each backslash
    body = re.sub(r'\\.', '', body)
    # Remove special characters
    body = re.sub(r'[^A-Za-z0-9\s]+', ' ', body)    
    # Remove everything after "Microsoft Teams meeting"
    body = re.split(r'Microsoft Teams', body, flags=re.IGNORECASE)[0]
    # Remove single letters (except 'I' and 'a')
    body = re.sub(r'\b(?!I\b|a\b)[A-Za-z]\b', '', body)
    # Remove extra whitespace
    body = ' '.join(body.split())
    return body.strip()

calendarDict = {}
item = 0

for indx, a in enumerate(calendar):
    subject = str(a.Subject)
    organizer = str(a.Organizer)
    meetingDate = str(a.Start)
    start_datetime = parse(meetingDate)
    date = start_datetime.date()
    time = start_datetime.time()
    duration = str(a.duration)
    body = str(a.Body.encode("utf8"))
    body = clean_body(body)
    participants = []
    for r in a.Recipients:
        participants.append(str(r))
    
    calendarDict[item] = {
        'Duration': duration, 
        'Organizer': organizer, 
        'Subject': subject, 
        'Date': date.strftime('%m/%d/%Y'),
        'Time': time.strftime('%H:%M:%S'),
        'Agenda': body,
        'Participants': ', '.join(participants)
    }
    item = item + 1

#Convert to string for Gemini
calendar_str = json.dumps(calendarDict)

#Load API key
load_dotenv()
GOOGLE_API_KEY=os.getenv('GOOGLE_API_KEY')
genai.configure(api_key=GOOGLE_API_KEY)

# Create the model
generation_config = {
  "temperature": 1,
  "top_p": 0.95,
  "top_k": 64,
  "max_output_tokens": 8192,
}

model = genai.GenerativeModel(
  model_name="gemini-1.5-flash",
  generation_config=generation_config,
)

chat_session = model.start_chat(
  history=[
    {
      "role": "user",
      "parts": calendar_str,
    }
  ]
)

chat_session.send_message('This is my meeting calendar for this week and next week. Review my meeting calendar. Summarize my meetings and provide any insights that you can find. Seperate your finding by past and future.')

print(chat_session.last.text)

import datetime
import streamlit as st
from snowflake.snowpark import Session
from snowflake.core import Root
import arxiv
import os
import base64
from snowflake.cortex import Complete

NUM_CHUNKS = 3
slide_window = 7

CORTEX_SEARCH_DATABASE = "ARXIV_RAG"
CORTEX_SEARCH_SCHEMA = "ARXIV_DATA"
CORTEX_SEARCH_SERVICE = "ARXIV_SEARCH_SERVICE"

COLUMNS = [
    "chunk",
    "relative_path"
]
st.set_page_config(layout="wide")

def create_session():
    connection_parameters = {
        "account": st.secrets["ragnroll_connection"]["account"],
        "user": st.secrets["ragnroll_connection"]["user"],
        "password": st.secrets["ragnroll_connection"]["password"],
        "warehouse": st.secrets["ragnroll_connection"]["warehouse"],
        "database": st.secrets["ragnroll_connection"]["database"],
        "schema": st.secrets["ragnroll_connection"]["schema"]
    }
    return Session.builder.configs(connection_parameters).create()

session = create_session()
root = Root(session)
my_stage_res = root.databases["ARXIV_RAG"].schemas["ARXIV_DATA"].stages["RESEARCH"]
svc = root.databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[CORTEX_SEARCH_SERVICE]

def init_session_state():
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if 'current_paper' not in st.session_state:
        st.session_state.current_paper = None
    if 'papers' not in st.session_state:
        st.session_state.papers = []
    if "pdf_path" not in st.session_state:
        st.session_state.pdf_path = None
    if "uploaded_papers" not in st.session_state:
        st.session_state.uploaded_papers = set()
    if "max_results" not in st.session_state:
        st.session_state.max_results = 1
    if "user_query" not in st.session_state:
        st.session_state.user_query = None
    if "start_year" not in st.session_state:
        st.session_state.start_year = 1991
    if "end_year" not in st.session_state:
        st.session_state.end_year = datetime.datetime.now().year

def fetch_papers(query, max_results, start_year, end_year):
    start_date = f"{start_year}0101"
    end_date = f"{end_year}1231"
    client = arxiv.Client()
    query_date = f"{query} AND submittedDate:[{start_date} TO {end_date}]"
    search = arxiv.Search(
        query=query_date,
        max_results=max_results,
        sort_by=arxiv.SortCriterion.Relevance,
    )
    papers = list(client.results(search))
    return papers

def get_similar_chunks(query):
    
    sample_paths = session.sql(f"""
        SELECT DISTINCT RELATIVE_PATH 
        FROM {CORTEX_SEARCH_DATABASE}.{CORTEX_SEARCH_SCHEMA}.RESEARCH_CHUNKS_TABLE
        LIMIT 5
    """).collect()
    st.sidebar.text("Sample paths in database:")
    for row in sample_paths:
        st.sidebar.text(row['RELATIVE_PATH'])

        
    st.sidebar.text(f"PDF path in session: {st.session_state.pdf_path}")
    st.sidebar.text(f"Path after slicing: {st.session_state.pdf_path[6:]}")

    filter_obj = {"@eq": {"relative_path": st.session_state.pdf_path[6:]} }
    response = svc.search(query, COLUMNS, filter=filter_obj, limit=NUM_CHUNKS)

    st.sidebar.json(response.json())
    return response.json()

def get_chat_history():
    chat_history = []
    
    start_index = max(0, len(st.session_state.messages) - slide_window)
    for i in range (start_index , len(st.session_state.messages) -1):
         chat_history.append(st.session_state.messages[i])

    return chat_history

def summarize_question_with_history(chat_history, question):

    prompt = f"""
        Based on the chat history below, the paper details, and the user's question, generate a query that extend the question
        with the chat history provided. The query should be in natural language. 
        Answer with only the query. Do not add any explanation. 
        <Paper Details>
        Title: {st.session_state.current_paper.title}
        Abstract: {st.session_state.current_paper.summary}
        </Paper Details>
        <chat_history>
        {chat_history}
        </chat_history>
        <question>
        {question}
        </question>
        """
    
    sumary = Complete('mistral-large2', prompt, session=session)   

    st.sidebar.text("Summary to be used to find similar chunks in the docs:")
    st.sidebar.caption(sumary)

    sumary = sumary.replace("'", "")

    return sumary

def create_prompt (myquestion):

    chat_history = get_chat_history()

    if chat_history != []: 
        question_summary = summarize_question_with_history(chat_history, myquestion)
        prompt_context =  get_similar_chunks(question_summary)
    else:
        prompt_context = get_similar_chunks(myquestion) #First question when using history
  
    prompt = f"""
           You are an expert academic researcher specializing in analyzing research papers. You are currently discussing the paper: "{st.session_state.current_paper.title}"

           Role: You are a knowledgeable guide helping users understand this specific research paper. You have deep expertise in the paper's content and can explain complex concepts clearly.

           Context Rules:
           - Base your answers solely on the provided paper context between <context></context> tags
           - Consider the conversation history between <chat_history></chat_history> tags for continuity
           - If information isn't in the context, say: "Based on the available sections of the paper, I cannot answer this specific question."
           - Never mention that you're using context or chat history

           Response Guidelines:
            - Be precise and academic in your explanations
            - Support answers with specific details from the paper
            - Use clear, professional language
            - Connect new information with previously discussed points when relevant
            - Break down complex concepts into understandable parts
            - Stay focused on the paper's content and findings
            - Ensure mathematical expressions are correctly formatted using LaTeX-style syntax (e.g., wrap inline math expressions with `$...$` and block math with `$$...$$` for proper rendering)
            - Provide clear descriptions of equations and symbols when introducing them, to aid comprehension
            - Format responses for clarity, with headings or bullet points if helpful for readability

           <chat_history>
           {chat_history}
           </chat_history>

           <context>          
           {prompt_context}
           </context>

           <question>  
           {myquestion}
           </question>

           Answer (maintaining academic tone and paper-specific focus):
           """
    

    return prompt


def answer_question(myquestion):

    prompt = create_prompt(myquestion)

    response = Complete('mistral-large2', prompt, session=session)   

    return response

def display_pdf(pdf_path):
    with open(pdf_path, "rb") as f:
        base64_pdf = base64.b64encode(f.read()).decode('utf-8')
    css = """
        <style>
            .pdf-container {
                width: 100%;
                height: 100vh;
                max-height: calc(100vh - 100px);
                overflow: hidden;
                border: 1px solid #ccc;
                border-radius: 4px;
            }
            .pdf-container iframe {
                width: 100%;
                height: 100%;
                border: none;
            }
        </style>
    """
    pdf_display = f"""
    {css}
        <div class="pdf-container">
            <iframe
                src="data:application/pdf;base64,{base64_pdf}"
                type="application/pdf">
            </iframe>
        </div>
    """
    st.markdown(pdf_display, unsafe_allow_html=True)

def display_paper_chat(paper):
    
    left_column, right_column = st.columns([5, 5])
    with left_column:
        st.markdown(f"### 📄 {paper.title}")
        
        try:
            display_pdf(st.session_state.pdf_path)
            if st.session_state.pdf_path not in st.session_state.uploaded_papers:
                my_stage_res.put(st.session_state.pdf_path, "/", auto_compress=False, overwrite=True)
                st.session_state.uploaded_papers.add(st.session_state.pdf_path)
                st.success(f"Successfully uploaded {paper.title} to the staging area.")
        except Exception as e:
            st.error(f"Error displaying PDF: {str(e)}")
            st.markdown(f"You can view the paper directly on [arXiv]({paper.entry_id})")
    with right_column:
        
        messages = st.container()


        for message in st.session_state.messages:
            with messages.chat_message(message["role"]):
                st.markdown(message["content"])

        # Accept user input
        if prompt := st.chat_input("What do you want to know about your products?"):
            # Add user message to the session state
            st.session_state.messages.append({"role": "user", "content": prompt})

            
            with messages.chat_message("user"):
                st.markdown(prompt)

            # Generate assistant's response
            with messages.chat_message("assistant"):
                with st.spinner("Thinking..."):
                    response = answer_question(prompt)  
                st.markdown(response)

            
            st.session_state.messages.append({"role": "assistant", "content": response})
            
            
def reset_chat():
    st.session_state.pdf_path = None
    st.session_state.current_paper = None
    st.session_state.messages = []

def init_chat(paper):
    st.session_state.current_paper = paper
    pdf_path = paper.download_pdf()
    saved_pdf_path = os.path.join("files", os.path.basename(pdf_path))
    os.makedirs("files", exist_ok=True)  
    with open(saved_pdf_path, "wb") as f:
        with open(pdf_path, "rb") as downloaded_pdf:
            f.write(downloaded_pdf.read())
    st.session_state.pdf_path = saved_pdf_path
    st.session_state.messages = [] 

def fetch_and_chat_callback():
    max_results = st.session_state.max_results
    user_query = st.session_state.user_query
    start_year = st.session_state.start_year
    end_year = st.session_state.end_year
    if user_query:
        st.write(f"Fetching {max_results} papers related to '{user_query}'")
        papers = fetch_papers(user_query, max_results, start_year, end_year)
        st.session_state.papers = papers
        st.success("Success")

def main():
    init_session_state()
    st.sidebar.expander("Session State").write(st.session_state)
    st.title("Arxchive: Chat with any research paper")

    
    if st.session_state.current_paper is not None:
        
        st.button("← Back to Search", on_click=reset_chat)
        display_paper_chat(st.session_state.current_paper)

    else:
        # Search interface
        use_date_filter = st.checkbox("Filter by date range", value=False)

        with st.form(key="query_form"):
            st.session_state.user_query = st.text_input("Enter your research topic/title")
            st.session_state.max_results = st.number_input("Number of papers to fetch", min_value=1, max_value=5, value=1)
            
            if use_date_filter:
                col1, col2 = st.columns(2)
                current_year = datetime.datetime.now().year
                with col1:
                    st.session_state.start_year = st.number_input(
                        "Start Year", 
                        min_value=1991,
                        max_value=current_year,
                        value=current_year - 3
                    )
                with col2:
                    st.session_state.end_year = st.number_input(
                        "End Year",
                        min_value=1991,
                        max_value=current_year,
                        value=current_year
                    )
                st.form_submit_button("Fetch and Chat", on_click=fetch_and_chat_callback())
            else:
                st.form_submit_button("Fetch and Chat", on_click=fetch_and_chat_callback())
                st.session_state.start_year = 1991
                st.session_state.end_year = datetime.datetime.now().year
        # Display papers
        if st.session_state.papers:
            for paper in st.session_state.papers:

                with st.container():
                    st.markdown("---")
                    st.markdown(f"### 📄 {paper.title}")
                    st.markdown("**Abstract:**")
                    st.markdown(f"{paper.summary}")
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown(f"**Published:** {paper.published}")
                    with col2:
                        st.markdown(f"**Authors:** {', '.join(author.name for author in paper.authors)}")
                    st.markdown(f"[View on arXiv]({paper.entry_id})")
                    st.button("Chat With Paper", key=f"chat_button_{paper.entry_id}", on_click=lambda:init_chat(paper))
                        

if __name__ == "__main__":
    main()
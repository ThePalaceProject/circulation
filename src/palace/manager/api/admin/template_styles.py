body_style: str = """
    margin: 10vh auto;
    font-family: 'Open Sans',Helvetica,Arial,sans-serif;
    padding: 25px 15px;
    width: 40vw;
    color: #403d37;
    border: 1px solid #DDD;
    border-radius: 4px;
    display: flex;
    flex-direction: column;
    align-items: center;
"""

label_style: str = """
    font-weight: 700;
"""

error_style: str = (
    body_style
    + """
    border-color: #D0343A;
"""
)
input_style: str = """
    border-radius: .25em;
    display: block;
    padding: 10px;
    border: 1px solid #403d37;
    box-shadow: none;
    font-size: 1rem;
    margin: 1vh 0 3vh 0;
    width: 25vw;
"""

section_style: str = """
    width: 25vw;
    padding: 12px;
    display: flex;
    justify-content: space-between;
    align-items: center;
"""
button_style: str = """
    background: #242DAB;
    border-color: transparent;
    border-radius: .25em;
    color: #fff;
    padding: 10px;
    font-family: 'Open Sans',Helvetica,Arial,sans-serif;
    font-size: 1rem;
    cursor: pointer;
    display: block;
    width: 100%;
    margin: 2vh auto;
"""

link_style: str = """
    background: #242DAB;
    text-align: center;
    text-decoration: none;
    border-color: #242DAB;
    border-radius: .25em;
    color: #fff;
    padding: 10px;
    font-size: 1rem;
    cursor: pointer;
    display: block;
    width: 25vw;
    margin: 2vh auto;
"""

small_link_style: str = (
    link_style
    + """
    width: 5vw;
    margin-bottom: 0;
"""
)

hr_style: str = """
    width: 10vw;
    margin: 3px 0 0 0;
    border: none;
    border-bottom: 1px solid #403d37;
"""

logo_style: str = """
    width: 200px;
    margin: 20px;
"""

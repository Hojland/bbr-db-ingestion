# Henter basebilledet  
FROM harbor.aws.c.dk/datascience/base/python-prod:latest

# Kopiere alle repositoriets filer ind og sætter palpatine som ejer. 
COPY --chown=palpatine:palpatine . . 

# Installere requirements_prod.txt. Hvor ligger den?
RUN pip install -r requirements_prod.txt --no-cache-dir

# Sætter brugeren til at være palpatine. Men vi kører ikke fixuid?
USER palpatine:palpatine

# Vi er kun interesserede i at fikse for fixuid under dev billedet fordi det (for det meste) kun giver mening at mounte volumes og at fixuid kun fikser et problem for volume mount. 

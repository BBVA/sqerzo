import os
import pickle
import random
import string
import datetime

ran_id = lambda: "".join(
    random.choice(string.ascii_letters + string.digits) for _ in range(80)
)

ran_text = lambda: "".join(
    random.choice(string.ascii_letters + string.digits + " ") for _ in range(100)
)

def main(count: 100):
    here = os.path.dirname(__file__)

    with open(os.path.join(here, "names.txt")) as f:
        names = f.read().splitlines()
    with open(os.path.join(here, "surnames.txt")) as f:
        sur_names = f.read().splitlines()

    # Generates 200 Users
    users = []
    for _ in range(200):
        name = f"{random.choice(names)} " \
               f"{random.choice(sur_names)} "

        email = name.replace("á", "a").\
            replace("é", "e").replace("í", "i").replace("ó", "o").\
            replace("ú", "u").replace(" ", ".").lower()

        users.append((name, email))


    mail_db = {}
    for _ in range(count):
        mail_id = ran_id()
        mail_from = random.choice(users)

        to = []
        for _ in range(random.randint(1, 5)):
            mail_to = random.choice(users)
            to.append(dict(zip(("name", "email"), mail_to)))

        cc = []
        for _ in range(random.randint(0, 4)):
            mail_cc = random.choice(users)
            cc.append(dict(zip(("name", "email"), mail_cc)))

        content = {
            "Message-Id": mail_id,
            "Date": str(datetime.datetime.now()),
            "From": dict(zip(("name", "email"), mail_from)),
            "Subject": ran_text(),
            "CC": cc,
            "To": to,
        }

        mail_db[mail_id] = content

    #
    # Add some Replies to email
    #
    mail_ids = list(mail_db.keys())
    for _ in range(500):
        mail_id = random.choice(mail_ids)
        in_reply_to_mail = random.choice(mail_ids)

        mail_db[mail_id]["In-Reply-To"] = in_reply_to_mail

    with open("mail.db", "wb") as f:
        pickle.dump(mail_db, f)


if __name__ == '__main__':
    main(50)

from __future__ import annotations

import pickle
from datetime import datetime
from dataclasses import dataclass

from sqerzo import GraphEdge, GraphNode, SQErzoGraph

@dataclass
class PersonNode(GraphNode):
    __keys__ = ("email",)

    name: str = None
    email: str = None

    def __post_init__(self):
        if not self.name:
            self.name = self.email


@dataclass
class MailNode(GraphNode):
    __keys__ = ("message_id",)
    __unique__ = ("identity", "message_id")

    subject: str = None
    date: datetime = None
    message_id: str = None

    def __post_init__(self):

        if type(self.date) is str:
            q = "%d %b %Y %H:%M:%S %z"
            if self.date.endswith(")"):
                q = f"{q} (%Z)"

            if self.date.find(",") != -1:
                q = f"%a, {q}"

            try:
                self.date = datetime.strptime(self.date, q)
            except ValueError:
                # TODO
                # date time (EDT)
                pass

class SentEdge(GraphEdge):
    pass

class ReplyToEdge(GraphEdge):
    pass

class ToEdge(GraphEdge):
    pass

class CCEdge(GraphEdge):
    pass


def create_graph(database_path: str, db_type: str, count: int = 50):
    gh = SQErzoGraph(db_type)
    gh.truncate()

    #
    # Clean the graph
    #
    print("[*] Cleaning graph")
    with open(database_path, "rb") as f:
        mail_database = pickle.load(f)

    # -------------------------------------------------------------------------
    # get_or_create each email
    # -------------------------------------------------------------------------

    # [ (source identity, in_reply) ]
    in_reply_rel = []

    print("[*] Building graph...    ")
    c = 1
    total = count or len(mail_database)


    with gh.transaction() as tx:

        for mail_id, user_email in mail_database.items():

            if c > count:
                break

            # -------------------------------------------------------------------------
            # Stores mail
            # -------------------------------------------------------------------------
            email = MailNode(
                subject=user_email["Subject"],
                date=user_email["Date"],
                message_id=user_email["Message-Id"]
            )
            tx.add(email)

            #
            # Check if mail is a reply of other, link them
            #
            if reply_to := user_email.get("In-Reply-To", None):
                in_reply_rel.append((email, reply_to))

            from_person = PersonNode(
                name=user_email["From"]["name"],
                email=user_email["From"]["email"]
            )
            tx.add(from_person)

            # -------------------------------------------------------------------------
            # From Person -[Sent]-> Email
            # -------------------------------------------------------------------------
            from_rel_mail = SentEdge(
                source=from_person,
                destination=email,
            )
            tx.add(from_rel_mail)

            # -------------------------------------------------------------------------
            # Stores To
            # -------------------------------------------------------------------------
            for to in user_email["To"]:
                to_person = PersonNode(
                    name=to["name"],
                    email=to["email"]
                )
                tx.add(to_person)

                # -------------------------------------------------------------------------
                # Email -[To]-> Person
                # -------------------------------------------------------------------------
                to_rel_mail = ToEdge(
                    source=email,
                    destination=to_person
                )
                tx.add(to_rel_mail)

            # -------------------------------------------------------------------------
            # Stores Mail CC
            # -------------------------------------------------------------------------
            for user_to in user_email.get("CC", []):
                p = PersonNode(
                    email=user_to["email"],
                    name=user_to["name"]
                )
                tx.add(p)

                # -------------------------------------------------------------------------
                # Email -[CC]-> Person
                # -------------------------------------------------------------------------
                to_rel = CCEdge(
                    source=email,
                    destination=p
                )
                tx.add(to_rel)

    return
    #
    # Link emails
    #
    for source_node, dst_in_reply in in_reply_rel:
        if reply_node := gh.fetch_one(MailNode, {"message_id": dst_in_reply}):
            reply_node.add_label("Reply")

            gh.update(reply_node)

            #
            # Email Source -[Reply]-> Reply Email
            #
            ed = ReplyToEdge(source_node, reply_node)
            gh.get_or_create(ed)

    print("[*] Done")


if __name__ == '__main__':
    # create_graph("mail.db", "redis://127.0.0.1:7000/?graph=gmail")
    # create_graph("mail.db", "neo4j://neo4j:s3cr3t@127.0.0.1:7687/?graph=gmail")

    create_graph("gmail.db", "redis://127.0.0.1:7000/?graph=gmail")
    create_graph("gmail.db", "neo4j://neo4j:s3cr3t@127.0.0.1:7687/?graph=gmail")
    # create_graph("mail.db", "gremlin://127.0.0.1?graph=gmail")

import random
class Dino:
    def __init__(self,name,time,diet,weight,size):
        self.name = name
        self.time = time
        self.diet = diet
        self.weight = weight
        self.size = size

    def describe(self):
        description = (f"Name: {self.name}, Period of living: {self.time}, Weight: {self.weight}, "
                      f"Size: {self.size}, Diet: {self.diet} ")
        print(f"Description of {self.name}: {description}" )

class Question:
    def __init__(self,type,topic,content,answer,options):
        self.type = type
        self.topic = topic
        self.content = content
        self.answer = answer
        self.options = options

class Quiz:
    def __init__(self,questions):
        self.questions = questions
        self.score = 0
        self.current_question = 0
        self.total_question = len(questions)

    def show_score(self):
        return print(f"Actual score: {self.score}")

    def ask_type(self,question):
        if question.type == "choice":
            print(question.content)
            for option in question.options:
                print(option)
            print("Put A/B/C/D as an answer")
            user_answer = input()
            return user_answer
        elif question.type == "tf":
            print(question.content)
            print("Put True or False")
            user_answer = input()
            return user_answer
        else:
            print(question.content)
            print("Put your answer in console")
            user_answer = input()
            return str(user_answer)

    def check_type(self,user_answer,question):
        if question.type == "choice":
            if user_answer.upper() == question.answer.upper():
                return True
            else:
                return False
        if question.type == "tf":
            if user_answer.lower() == question.answer.lower():
                return True
            else:
                return False
        if question.type == "text":
            if user_answer.strip().lower() == question.answer.strip().lower():
                return True
            else:
                return False


    def ask_question(self,question):
        return self.ask_type(question)

    def check_answer(self,question,user_answer):
        if self.check_type(user_answer,question) == True:
            self.score += 1
            print("Correct answer!")
        else:
            print("Incorrect answer")
            print(f"The correct answer is {question.answer}")

    def start(self):
        random.shuffle(self.questions)
        for question in self.questions:
            user_answer = self.ask_question(question)
            self.check_answer(question, user_answer)
            print(f"Current score: {self.score}")
        print(f"Your final score is {self.score}/{self.total_question}")




Trex = Dino("Tyrannosaurus Rex", "Late Cretaceous", "Carnivore", 8, 12)
# Weight: 8 tons (t), Size: 12 meters (m)
Triceratops = Dino("Triceratops", "Late Cretaceous", "Herbivore", 6, 9)
# Weight: 6 tons (t), Size: 9 meters (m)
Brachiosaurus = Dino("Brachiosaurus", "Late Jurassic", "Herbivore", 56, 26)
# Weight: 56 tons (t), Size: 26 meters (m)
import json
def load_questions_from_json(filepath):
    with open(filepath, "r") as file:
        data = json.load(file)
        return [Question(**item) for item in data]

questions = load_questions_from_json("question.json")
quiz = Quiz(questions)
quiz.start()
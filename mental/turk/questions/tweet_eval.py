
from boto.mturk.question import *

__all__ = ["TweetEvaluationQuestion"]

class TweetEvaluationQuestion:
    def __init__(self, tweets):
        self.tweets = tweets

        self.overview = self.create_overview()
        self.questions = self.create_questions()
        self.form = self.create_question_form()
    
    def create_overview(self):
        overview = Overview()
        overview.append_field('Title', 'Answer a quick survey about these tweets.')
        overview.append(SimpleField('Text',
            'Please rate these tweets according to whether they show depression, '
            'anxiety, or are unrelated.'))
        overview.append(FormattedContent(
            "<ul>"
            "    <li>Answer both the survey for depression and the survey for anxiety.</li>"
            "    <li>A tweet can indicate both depression and anxiety - the two survey answers are not mutually exclusive.</li>"
            "    <li>The ambiguous option means you can't clearly tell if the tweet is &quot;Depressed&quot;/&quot;Cheerful&quot; or &quot;Anxious&quot;/&quot;Relaxed&quot;.</li>"
            "    <li>The unrelated option means it has nothing to do with these topics.</li>"
            "    <li>You are not required to follow any links that maybe be included in the tweet text.</li>"
            "    <li>When in doubt, here are definitions to use when evaluating the tweets:</li>"
            "    <li>Depression: &quot;a period of unhappiness or low morale which lasts longer than several weeks and may include ideation of self-inflicted injury or suicide&quot;</li>"
            "    <li>Anxiety: &quot;An unpleasant state of mental uneasiness, nervousness, apprehension and obsession or concern about some uncertain event.&quot;</li>"
            "</ul>"))
        
        return overview

    def create_questions(self):
        questions = []
        depression_selections = [
            ('Depressed', 'depressed'),
            ('Cheerful',  'cheerful'),
            ('Ambiguous', 'ambiguous'),
            ('Unrelated', 'unrelated')]
        anxiety_selections = [
            ('Anxious',   'anxious'),
            ('Relaxed',   'relaxed'),
            ('Ambiguous', 'ambiguous'),
            ('Unrelated', 'unrelated')]

        for tweet in self.tweets:
            """ DEPRESSION """
            """ question content (the tweet) """
            depression_qc = QuestionContent()
            # strip away unicode characters
            depression_qc.append(SimpleField('Text', tweet["text"].encode("utf-8").decode("ascii", "ignore")))

            """ selections """
            depression_sel = SelectionAnswer(
                min = 1, max = 1,
                style = 'radiobutton',
                selections = depression_selections,
                type = 'text')

            questions += [Question(
                identifier = "tweet_%03d_depression" % self.tweets.index(tweet),
                content = depression_qc,
                answer_spec = AnswerSpecification(depression_sel),
                is_required = True)]

            """ ANXIETY """
            """ question content (the tweet) """
            anxiety_qc = QuestionContent()
            # strip away unicode characters
            anxiety_qc.append(SimpleField('Text', tweet["text"].encode("utf-8").decode("ascii", "ignore")))

            """ selections """
            anxiety_sel = SelectionAnswer(
                min = 1, max = 1,
                style = 'radiobutton',
                selections = anxiety_selections,
                type = 'text')

            questions += [Question(
                identifier = "tweet_%03d_anxiety" % self.tweets.index(tweet),
                content = anxiety_qc,
                answer_spec = AnswerSpecification(anxiety_sel),
                is_required = True)]

        return questions

    def create_question_form(self):
        form = QuestionForm()
        form.append(self.overview)
        for question in self.questions:
            form.append(question)

        return form

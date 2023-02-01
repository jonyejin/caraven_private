from unittest import TestCase
from utils import remove_reporter_name

class Test(TestCase):
    def test_remove_reporter_name(self):
        self.assertEqual(remove_reporter_name("[서울=뉴시스]이재준 기자 = 올해..."), "올해...")
        self.assertEqual(remove_reporter_name("[세종=이데일리 이진철 기자] 정세균"), "정세균")
        self.assertEqual(remove_reporter_name("[더팩트 | 서재근 기자]"), "")
        self.assertEqual(remove_reporter_name("(서울=뉴스1) 김태환 기자,음상준 기자,이영성 기자,이형진 기자 = 정부가"), "정부가")
        self.assertEqual(remove_reporter_name("(런던=연합뉴스) 박대한 특파원 = 유럽 최대"), "유럽 최대")
        self.assertEqual(remove_reporter_name("(서울=연합뉴스) 옥철 기자 =  미국 항공기 제조업체 보잉이 250억 달러(약 30조5천억원) 규모의 회사채를 발행하기로 하면서 당장 미 정부의 지원을 추진할 계획이 없다고 지난달 30일(현지시간) 밝혔다."),"미국 항공기 제조업체 보잉이 250억 달러(약 30조5천억원) 규모의 회사채를 발행하기로 하면서 당장 미 정부의 지원을 추진할 계획이 없다고 지난달 30일(현지시간) 밝혔다.")
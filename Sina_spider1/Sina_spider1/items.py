# encoding=utf-8

from scrapy import Item, Field


class InformationItem(Item):
    """ 个人信息 """
    _id = Field()  # 用户ID
    NickName = Field()  # 昵称
    Gender = Field()  # 性别
    Province = Field()  # 所在省
    City = Field()  # 所在城市
    Signature = Field()  # 个性签名
    Birthday = Field()  # 生日
    Num_Tweets = Field()  # 微博数
    Num_Follows = Field()  # 关注数
    Num_Fans = Field()  # 粉丝数
    Sex_Orientation = Field()  # 性取向
    Marriage = Field()  # 婚姻状况
    URL = Field()  # 首页链接


class TweetsItem(Item):
    """ 微博信息 """
    _id = Field()  # 用户ID-微博ID
    ID = Field()  # 用户ID
    Content = Field()  # 微博内容
    PubTime = Field()  # 发表时间
    Co_oridinates = Field()  # 定位坐标
    Tools = Field()  # 发表工具/平台
    Like_num = Field()  # 点赞数
    Comment_num = Field()  # 评论数
    Transfer_num = Field()  # 转载数
    All_comment = Field()  #评论数
   # All_transfer = Field()


class FollowsItem(Item):
    """ 关注人列表 """
    _id = Field()   # 用户ID
    follows = Field()  # 关注

class FansItem(Item):
    """ 粉丝列表 """
    _id = Field()  # 用户ID
    fans = Field()  # 粉丝

class CommentItem(Item):
    """ 评论信息"""
#    _id = Field()  # 用户ID-微博ID
    Fan_id = Field()
    Fan_name = Field()
    Comment = Field()  #评论内容
    Zan_num = Field()
    Comment_time = Field()

class TransferItem(Item):
    """ 转发数据"""
 #   _id = Field()  # 用户ID-微博ID
    Fan_id = Field()
    Fan_name = Field()
    Transfer = Field()  # 评论内容
    Zan_num = Field()
    Transfer_time = Field()
import React, { Component, PropTypes } from 'react';
import withStyles from 'isomorphic-style-loader/lib/withStyles';
import s from './Inbox.css';
import flatAvatar from '../../../../common/images/flat-avatar.png';

const title = 'Inbox';

class Inbox extends Component {
  static contextTypes = {
    setTitle: PropTypes.func.isRequired,
  };

  componentWillMount() {
    this.context.setTitle(title);
  }
  render() {
    return (
      <div className="conter-wrapper animate">
        <div className={`inbox-container-wrap ${s.inboxContainerWrap}`}>
          <div className={`inbox-container ${s.inboxContainer}`}>
            <div className={`col email-options ps-container ${s.emailOptions}`}>
              <div className="padding-15">
                <div className={`butt-container ${s.buttContainer}`}>
                  <a
                    className={
                      `btn btn-primary btn-block btn-rounded ${s.btnBlock} ${s.btnStyle}`
                    }
                  >Compose</a>
                </div>
                <ul className={`main-options ${s.mainOptions}`}>
                  <li className={`activeli ${s.activeLi}`}>
                    <a>
                      <span className="title"> &nbsp; Inbox</span>
                      <span className="badge pull-right">10</span>
                    </a>
                  </li>
                  <li>
                    <a>
                      <span className="title"> &nbsp; Junk Mail</span>
                    </a>
                  </li>
                  <li>
                    <a>
                      <span className="title"> &nbsp; Drafts</span>
                      <span className="badge pull-right">16</span>
                    </a>
                  </li>
                  <li>
                    <a>
                      <span className="title"> &nbsp; Sent</span>
                    </a>
                  </li>
                  <li>
                    <a>
                      <span className="title"> &nbsp; Trash</span>
                    </a>
                  </li>
                  <hr className={`poor ${s.poor}`} />
                  <h5>LABELS</h5>
                  <li>
                    <a>
                      <span className="title"> &nbsp; Clients
                        <i
                          className={
                            `fa fa-stop pull-right faorange ${s.faStop} ${s.faOrange}`
                          }
                        />
                      </span>
                    </a>
                  </li>
                  <li>
                    <a>
                      <span className="title"> &nbsp; Social
                        <i
                          className={
                            `fa fa-stop pull-right fayellow ${s.faStop} ${s.faYellow}`
                          }
                        />
                      </span>
                    </a>
                  </li>
                  <li>
                    <a>
                      <span className="title"> &nbsp; Family
                        <i
                          className={
                            `fa fa-stop pull-right facyan ${s.faStop} ${s.faCyan}`
                          }
                        />
                      </span>
                    </a>
                  </li>
                  <li>
                    <a>
                      <span className="title"> &nbsp; Friends
                        <i
                          className={
                            `fa fa-stop pull-right fapurple ${s.faStop} ${s.faPurple}`
                          }
                        />
                      </span>
                    </a>
                  </li>
                </ul>
              </div>
            </div>
          </div>

          <div className={`message-list-wrapper ${s.messageListWrapper}`}>
            <div className={`searchbox ${s.searchBox}`}>
              <span className={`sebox ${s.seBox}`}>Inbox</span> (14 unread)
              <i className="fa fa-envelope-o" />
              <input
                type="text"
                className="form-control"
                id="exampleInputEmail1"
                placeholder="Search Inbox..."
              />
            </div>
            <div className={`wrap-list ${s.wrapList}`}>
              <div className={`messages-list ${s.messagesList}`}>
                <ul>
                  <li className={`messages-item ${s.messagesItem}`}>
                    <a href="">
                      <div className={`leftist ${s.leftist}`}><i className="fa fa-star" />
                        <label className={`checkbox1 ${s.checkbox1}`} htmlFor="Option45">
                          <input id="Option45" type="checkbox" className="" />
                          <span />
                        </label>
                      </div>
                      <span className={`messages-item-subject ${s.messagesItemSubject}`}>
                        Nicole Bell
                      </span>
                      <span className={`date-class text-muted pull-right ${s.dateClass}`}>
                        <i className="fa fa-paperclip" />&nbsp;7th Jan
                      </span>
                      <span className={`messages-item-content ${s.messagesItemContent}`}>
                        <p>Hi Peter, Thanks for the e-mail. Lorem ipsum dolor sit amet,</p>
                      </span>
                    </a>
                  </li>
                  <li className={`messages-item ${s.messagesItem}`}>
                    <a href="">
                      <div className={`leftist ${s.leftist}`}><i className="fa fa-star" />
                        <label className={`checkbox1 ${s.checkbox1}`} htmlFor="Option45">
                          <input id="Option45" type="checkbox" className="" />
                          <span />
                        </label>
                      </div>
                      <span className={`messages-item-subject ${s.messagesItemSubject}`}>
                        Nicole Bell
                      </span>
                      <span className={`date-class text-muted pull-right ${s.dateClass}`}>
                        <i className="fa fa-paperclip" />&nbsp;7th Jan
                      </span>
                      <span className={`messages-item-content ${s.messagesItemContent}`}>
                        <p>Hi Peter, Thanks for the e-mail. Lorem ipsum dolor sit amet,</p>
                      </span>
                    </a>
                  </li>
                  <li className={`messages-item ${s.messagesItem}`}>
                    <a href="">
                      <div className={`leftist ${s.leftist}`}><i className="fa fa-star" />
                        <label className={`checkbox1 ${s.checkbox1}`} htmlFor="Option45">
                          <input id="Option45" type="checkbox" className="" />
                          <span />
                        </label>
                      </div>
                      <span
                        className={`messages-item-subject ${s.messagesItemSubject}`}
                      >
                        Nicole Bell
                      </span>
                      <span className={`date-class text-muted pull-right ${s.dateClass}`}>
                        <i className="fa fa-paperclip" />&nbsp;7th Jan
                      </span>
                      <span className={`messages-item-content ${s.messagesItemContent}`}>
                        <p>Hi Peter, Thanks for the e-mail. Lorem ipsum dolor sit amet,</p>
                      </span>
                    </a>
                  </li>
                  <li className={`messages-item ${s.messagesItem} ${s.activeMessage}`}>
                    <a href="">
                      <div className={`leftist ${s.leftist}`}><i className="fa fa-star" />
                        <label className={`checkbox1 ${s.checkbox1}`} htmlFor="Option45">
                          <input id="Option45" type="checkbox" className="" />
                          <span />
                        </label>
                      </div>
                      <span
                        className={`messages-item-subject ${s.messagesItemSubject}`}
                      >
                        Nicole Bell
                      </span>
                      <span className={`date-class text-muted pull-right ${s.dateClass}`}>
                        <i className="fa fa-paperclip" />&nbsp;7th Jan
                      </span>
                      <span className={`messages-item-content ${s.messagesItemContent}`}>
                        <p>Hi Peter, Thanks for the e-mail. Lorem ipsum dolor sit amet,</p>
                      </span>
                    </a>
                  </li>
                  <li className={`messages-item ${s.messagesItem}`}>
                    <a href="">
                      <div className={`leftist ${s.leftist}`}>
                        <i className={`fa fa-star ${s.faStarred}`} />
                        <label className={`checkbox1 ${s.checkbox1}`} htmlFor="Option45">
                          <input id="Option45" type="checkbox" className="" />
                          <span />
                        </label>
                      </div>
                      <span
                        className={`messages-item-subject ${s.messagesItemSubject}`}
                      >
                        Nicole Bell
                      </span>
                      <span className={`date-class text-muted pull-right ${s.dateClass}`}>
                        <i className="fa fa-paperclip" />&nbsp;7th Jan
                      </span>
                      <span className={`messages-item-content ${s.messagesItemContent}`}>
                        <p>Hi Peter, Thanks for the e-mail. Lorem ipsum dolor sit amet,</p>
                      </span>
                    </a>
                  </li>
                  <li className={`messages-item ${s.messagesItem}`}>
                    <a href="">
                      <div className={`leftist ${s.leftist}`}><i className="fa fa-star" />
                        <label className={`checkbox1 ${s.checkbox1}`} htmlFor="Option45">
                          <input id="Option45" type="checkbox" className="" />
                          <span />
                        </label>
                      </div>
                      <span
                        className={`messages-item-subject ${s.messagesItemSubject}`}
                      >
                        Nicole Bell
                      </span>
                      <span className={`date-class text-muted pull-right ${s.dateClass}`}>
                        <i className="fa fa-paperclip" />&nbsp;7th Jan
                      </span>
                      <span className={`messages-item-content ${s.messagesItemContent}`}>
                        <p>Hi Peter, Thanks for the e-mail. Lorem ipsum dolor sit amet,</p>
                      </span>
                    </a>
                  </li>
                  <li className={`messages-item ${s.messagesItem}`}>
                    <a href="">
                      <div className={`leftist ${s.leftist}`}><i className="fa fa-star" />
                        <label className={`checkbox1 ${s.checkbox1}`} htmlFor="Option45">
                          <input id="Option45" type="checkbox" className="" />
                          <span />
                        </label>
                      </div>
                      <span
                        className={`messages-item-subject ${s.messagesItemSubject}`}
                      >
                        Nicole Bell
                      </span>
                      <span className={`date-class text-muted pull-right ${s.dateClass}`}>
                        <i className="fa fa-paperclip" />&nbsp;7th Jan
                      </span>
                      <span className={`messages-item-content ${s.messagesItemContent}`}>
                        <p>Hi Peter, Thanks for the e-mail. Lorem ipsum dolor sit amet,</p>
                      </span>
                    </a>
                  </li>
                  <li className={`messages-item ${s.messagesItem}`}>
                    <a href="">
                      <div className={`leftist ${s.leftist}`}><i className="fa fa-star" />
                        <label className={`checkbox1 ${s.checkbox1}`} htmlFor="Option45">
                          <input id="Option45" type="checkbox" className="" />
                          <span />
                        </label>
                      </div>
                      <span
                        className={`messages-item-subject ${s.messagesItemSubject}`}
                      >
                        Nicole Bell
                      </span>
                      <span className={`date-class text-muted pull-right ${s.dateClass}`}>
                        <i className="fa fa-paperclip" />&nbsp;7th Jan
                      </span>
                      <span className={`messages-item-content ${s.messagesItemContent}`}>
                        <p>Hi Peter, Thanks for the e-mail. Lorem ipsum dolor sit amet,</p>
                      </span>
                    </a>
                  </li>
                  <li className={`messages-item ${s.messagesItem}`}>
                    <a href="">
                      <div className={`leftist ${s.leftist}`}><i className="fa fa-star" />
                        <label className={`checkbox1 ${s.checkbox1}`} htmlFor="Option45">
                          <input id="Option45" type="checkbox" className="" />
                          <span />
                        </label>
                      </div>
                      <span className={`messages-item-subject ${s.messagesItemSubject}`}>
                         Nicole Bell
                      </span>
                      <span className={`date-class text-muted pull-right ${s.dateClass}`}>
                        <i className="fa fa-paperclip" />&nbsp;7th Jan
                      </span>
                      <span className={`messages-item-content ${s.messagesItemContent}`}>
                        <p>Hi Peter, Thanks for the e-mail. Lorem ipsum dolor sit amet,</p>
                      </span>
                    </a>
                  </li>
                  <li className={`messages-item ${s.messagesItem}`}>
                    <a href="">
                      <div className={`leftist ${s.leftist}`}><i className="fa fa-star" />
                        <label className={`checkbox1 ${s.checkbox1}`} htmlFor="Option45">
                          <input id="Option45" type="checkbox" className="" />
                          <span />
                        </label>
                      </div>
                      <span className={`messages-item-subject ${s.messagesItemSubject}`}>
                         Nicole Bell
                      </span>
                      <span className={`date-class text-muted pull-right ${s.dateClass}`}>
                        <i className="fa fa-paperclip" />&nbsp;7th Jan
                      </span>
                      <span className={`messages-item-content ${s.messagesItemContent}`}>
                        <p>Hi Peter, Thanks for the e-mail. Lorem ipsum dolor sit amet,</p>
                      </span>
                    </a>
                  </li>
                  <li className={`messages-item ${s.messagesItem}`}>
                    <a href="">
                      <div className={`leftist ${s.leftist}`}><i className="fa fa-star" />
                        <label className={`checkbox1 ${s.checkbox1}`} htmlFor="Option45">
                          <input id="Option45" type="checkbox" className="" />
                          <span />
                        </label>
                      </div>
                      <span className={`messages-item-subject ${s.messagesItemSubject}`}>
                         Nicole Bell
                      </span>
                      <span className={`date-class text-muted pull-right ${s.dateClass}`}>
                        <i className="fa fa-paperclip" />&nbsp;7th Jan
                      </span>
                      <span className={`messages-item-content ${s.messagesItemContent}`}>
                        <p>Hi Peter, Thanks for the e-mail. Lorem ipsum dolor sit amet,</p>
                      </span>
                    </a>
                  </li>

                </ul>
              </div>
            </div>
          </div>
          <div className={`text-wrapper bg-white ${s.textWrapper}`}>
            <div className={`wrap-message ${s.wrapMessage}`}>
              <div className="n0-scroll">
                <div className={`message-topic ${s.messageTopic}`}>
                  <i className="fa fa-star m-r-xs text-primary" /> Development Files
                  <span className={`pull-right text-muted ${s.pullRight}`}>
                    <a href=""><i className="fa fa-reply" /></a>
                    <a href=""><i className="fa fa-trash-o" /></a>
                  </span>
                </div>
                <div className={`message-sender ${s.messageSender}`}>
                  <img
                    className={`img-circle sender-img m-r-xs ${s.senderImg}`}
                    src={flatAvatar}
                    alt=""
                  />
                  <a href="" onClick={(e) => { e.preventDefaut(); }}>Kumar Sanket</a> to
                  <a href="" onClick={(e) => { e.preventDefaut(); }}>me</a> &nbsp;
                  <i className="fa fa-caret-square-o-down" />
                  <small className="pull-right m-t-sm">
                    <i className="fa fa-paperclip" /> &nbsp;
                    <a href="" onClick={(e) => { e.preventDefaut(); }}>(2)</a>
                     &nbsp;&nbsp; Today 7:30 AM
                  </small>
                </div>
              </div>
              <div className={`message-content ${s.messageContent}`}>
                <p>Hi Peter,</p>
                <p>
                  Thanks for the e-mail. It is always nice to hear from people, especially from
                  you, Scott.
                </p>
                <p>I have not got any reply, a positive or negative one, from Seibido yet.
                  <br />Let's wait and hope that it will make a BOOK.
                </p>
                <p>
                  Have you finished your paperwork for Kaken and writing academic articles?<br />
                  If you have some free time in the near future, I want to meet you and explain to
                  you our next project.
                </p>
                <p>
                  Why not drink out in Hiroshima if we are accepted?<br />
                  We need to celebrate ourselves, don't we?<br />Let's have a small end-of-the-year
                  party!
                </p>
                <p>Sincerely, K. Nakagawa</p>
              </div>
              <div className={`messagefooter ${s.messageFooter}`}>
                <button
                  type="button"
                  className={`btn btn-primary btn-rounded pull-right ${s.pullRight} ${s.btnStyle}`}
                >
                  <i className="fa fa-reply" /> Reply
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }
}

export default withStyles(s)(Inbox);

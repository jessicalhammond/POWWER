import React, { Component, PropTypes } from 'react';
// import { PageHeader } from 'react-bootstrap';
import withStyles from 'isomorphic-style-loader/lib/withStyles';
// import Link from '../../../../components/Link';
import s from './Compose.css';

const title = 'Compose Message';

class Compose extends Component {
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

          <div className={`compose-container ${s.composeContainer}`}>
            <div className={`wrap-compose ${s.wrapCompose}`}>
              <div className={`mail-header ${s.mailHeader}`}>
                <h4>New Email</h4>
              </div>
              <div className={`receipient ${s.receipient}`}>
                <strong className={s.to}>TO </strong>
                <span className={`label label-primary ${s.labelPrimary}`}>john@doe.com</span>
              </div>
              <div className={`subject ${s.subject}`}>
                <strong className={`strong-header ${s.strongHeader}`}>SUBJECT</strong>
                <strong className={`subject ${s.subject}`}>[LOGO] Envelope</strong>
              </div>
              <text-angular ng-model="htmlVariable" />
              <textarea rows="19" className="form-control" />
              <div className={`send-footer ${s.sendFooter}`}>
                <button type="button" className="btn btn-primary btn-rounded">
                Send
                </button>
                &nbsp;&nbsp;&nbsp;<a href=""><i className="fa fa-paperclip" /></a>
                <a href=""><i className="fa fa-trash-o pull-right" /></a>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }
}

export default withStyles(s)(Compose);
